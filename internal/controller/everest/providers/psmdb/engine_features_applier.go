// everest-operator
// Copyright (C) 2022 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package psmdb

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/AlekSi/pointer"
	psmdbv1 "github.com/percona/percona-server-mongodb-operator/pkg/apis/psmdb/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	enginefeatureseverestv1alpha1 "github.com/percona/everest-operator/api/enginefeatures.everest/v1alpha1"
	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

const (
	splitHorizonExternalKey = "external"
	publicIPPendingValue    = "pending"
)

var (
	errShardingNotSupported = errors.New("sharding is not supported for SplitHorizon DNS feature")
	validityNotAfter        = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
)

// EngineFeaturesApplier is responsible for applying engine features to the PerconaServerMongoDB spec.
type EngineFeaturesApplier struct {
	*Provider
}

// NewEngineFeaturesApplier creates a new engineFeaturesApplier.
func NewEngineFeaturesApplier(p *Provider) *EngineFeaturesApplier {
	return &EngineFeaturesApplier{
		Provider: p,
	}
}

// ----------------- Features Appliers ----------------------

// ApplyFeatures applies the engine features to the PerconaServerMongoDB spec.
func (a *EngineFeaturesApplier) ApplyFeatures(ctx context.Context) error {
	if pointer.Get(a.DB.Spec.EngineFeatures).PSMDB == nil {
		// Nothing to do
		return nil
	}

	var err error
	if err = a.applySplitHorizonDNSConfig(ctx); err != nil {
		return err
	}
	return nil
}

// applySplitHorizonDNSConfig applies the SplitHorizon DNS configuration to the PerconaServerMongoDB spec.
func (a *EngineFeaturesApplier) applySplitHorizonDNSConfig(ctx context.Context) error { //nolint:funcorder
	database := a.DB
	psmdb := a.PerconaServerMongoDB
	shdcName := database.Spec.EngineFeatures.PSMDB.SplitHorizonDNSConfigName
	if shdcName == "" {
		// SplitHorizon DNS feature is not enabled
		return nil
	}

	if pointer.Get(database.Spec.Sharding).Enabled {
		// For the time being SplitHorizon DNS feature can be applied for unsharded PSMDB clusters only.
		// Later we may consider adding support for sharded clusters as well.
		return errShardingNotSupported
	}

	shdc := &enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{}
	if err := a.C.Get(ctx, types.NamespacedName{Namespace: a.DB.GetNamespace(), Name: shdcName}, shdc); err != nil {
		return err
	}

	shdcCaSecret := &corev1.Secret{}
	if err := a.C.Get(ctx, types.NamespacedName{Namespace: shdc.GetNamespace(), Name: shdc.Spec.TLS.SecretName}, shdcCaSecret); err != nil {
		return err
	}

	horSpec := psmdbv1.HorizonsSpec{}
	for i := range int(database.Spec.Engine.Replicas) {
		horSpec[fmt.Sprintf("%s-rs0-%d", database.GetName(), i)] = map[string]string{
			splitHorizonExternalKey: fmt.Sprintf("%s-rs0-%d-%s.%s",
				database.GetName(),
				i,
				database.GetNamespace(),
				shdc.Spec.BaseDomainNameSuffix),
		}
	}
	psmdb.Spec.Replsets[0].Horizons = horSpec

	needCreateSecret := false // do not generate server certificate on each reconciliation loop
	psmdbSplitHorizonSecretName := getSplitHorizonDNSConfigSecretName(database.GetName())

	psmdbSplitHorizonSecret := &corev1.Secret{}
	if err := a.C.Get(ctx, types.NamespacedName{Namespace: database.GetNamespace(), Name: psmdbSplitHorizonSecretName}, psmdbSplitHorizonSecret); err != nil {
		if k8serrors.IsNotFound(err) {
			needCreateSecret = true
		} else {
			return err
		}
	}

	if needCreateSecret {
		// Generate server TLS certificate for SplitHorizon DNS domains
		psmdbSplitHorizonDomains := []string{
			// Internal SANs
			"localhost",
			database.GetName() + "-rs0",
			fmt.Sprintf("*.%s-rs0", database.GetName()),
			fmt.Sprintf("%s-rs0.%s", database.GetName(), database.GetNamespace()),
			fmt.Sprintf("*.%s-rs0.%s", database.GetName(), database.GetNamespace()),
			fmt.Sprintf("%s-rs0.%s.svc.cluster.local", database.GetName(), database.GetNamespace()),
			fmt.Sprintf("*.%s-rs0.%s.svc.cluster.local", database.GetName(), database.GetNamespace()),
			// External SANs
			"*." + shdc.Spec.BaseDomainNameSuffix,
		}
		psmdbSplitHorizonServerCertBytes, psmdbSplitHorizonServerPrivKeyBytes, err := issueSplitHorizonCertificate(
			shdcCaSecret.Data["ca.crt"],
			shdcCaSecret.Data["ca.key"],
			psmdbSplitHorizonDomains)
		if err != nil {
			return fmt.Errorf("issue split-horizon server TLS certificate: %w", err)
		}

		// Store generated server TLS certificate in a secret. It is DB specific.
		psmdbSplitHorizonSecret.SetName(psmdbSplitHorizonSecretName)
		psmdbSplitHorizonSecret.SetNamespace(database.GetNamespace())
		if _, err = controllerutil.CreateOrUpdate(ctx, a.C, psmdbSplitHorizonSecret, func() error {
			psmdbSplitHorizonSecret.Data = map[string][]byte{
				"tls.crt": []byte(psmdbSplitHorizonServerCertBytes),
				"tls.key": []byte(psmdbSplitHorizonServerPrivKeyBytes),
				"ca.crt":  shdcCaSecret.Data["ca.crt"],
			}
			psmdbSplitHorizonSecret.Type = corev1.SecretTypeTLS

			if err = controllerutil.SetControllerReference(database, psmdbSplitHorizonSecret, a.C.Scheme()); err != nil {
				return err
			}

			return nil
		}); err != nil {
			return fmt.Errorf("failed to create server TLS certificate secret: %w", err)
		}
	}

	// set reference to secret with certificate for external domain
	if psmdb.Spec.Secrets == nil {
		psmdb.Spec.Secrets = &psmdbv1.SecretsSpec{}
	}
	psmdb.Spec.Secrets.SSL = psmdbSplitHorizonSecret.GetName()

	return nil
}

func getSplitHorizonDNSConfigSecretName(dbName string) string {
	return dbName + "-sh-cert"
}

// ----------------- Features Statuses ----------------------

// GetEngineFeaturesStatuses retrieves the statuses of the enabled engine features.
func (a *EngineFeaturesApplier) GetEngineFeaturesStatuses(ctx context.Context) (*everestv1alpha1.PSMDBEngineFeaturesStatus, bool) {
	logger := log.FromContext(ctx)
	if pointer.Get(a.DB.Spec.EngineFeatures).PSMDB == nil {
		// Nothing to do
		return nil, true
	}

	// Flag indicating whether all features statuses are ready.
	// If at least one feature status is not ready, the overall status is not ready.
	// This is used to determine whether we need to trigger reconciliation loop again in order to
	// obtain the EngineFeatures status.
	statusReady := true
	psmdbEFStatuses := &everestv1alpha1.PSMDBEngineFeaturesStatus{}

	if shStatus, ready, err := a.getSplitHorizonStatus(ctx); err != nil {
		logger.Error(err, "failed to get SplitHorizon PSMDB engine feature status")
		statusReady = false
	} else {
		psmdbEFStatuses.SplitHorizon = shStatus
		// Update overall statusReady flag.
		// It may appear that there is no error, but the status is not ready yet (e.g. waiting for services to be created).
		if !ready {
			statusReady = false
		}
	}

	// TODO: add other PSMDB engine features statuses here
	return psmdbEFStatuses, statusReady
}

func (a *EngineFeaturesApplier) getSplitHorizonStatus(ctx context.Context) (*enginefeatureseverestv1alpha1.SplitHorizonStatus, bool, error) {
	shdcName := pointer.Get(pointer.Get(a.DB.Spec.EngineFeatures).PSMDB).SplitHorizonDNSConfigName
	if shdcName == "" {
		// SplitHorizon DNS feature is not enabled
		return &enginefeatureseverestv1alpha1.SplitHorizonStatus{}, true, nil
	}

	shStatus := &enginefeatureseverestv1alpha1.SplitHorizonStatus{}
	shStatus.Domains = make([]enginefeatureseverestv1alpha1.SplitHorizonDomain, 0, len(a.PerconaServerMongoDB.Spec.Replsets[0].Horizons))
	connHosts := make([]string, 0, len(a.PerconaServerMongoDB.Spec.Replsets[0].Horizons))
	statusReady := true

	// Get generated external domains from PSMDB spec
	for podName, horData := range a.PerconaServerMongoDB.Spec.Replsets[0].Horizons {
		svc := &corev1.Service{}
		if err := a.C.Get(ctx, types.NamespacedName{Namespace: a.DB.GetNamespace(), Name: podName}, svc); err != nil {
			if err = client.IgnoreNotFound(err); err != nil {
				return nil, false, fmt.Errorf("get service %s for SplitHorizon status: %w", podName, err)
			}
			// service not created yet
			statusReady = false
			continue
		}

		domain := horData[splitHorizonExternalKey]
		connHosts = append(connHosts, net.JoinHostPort(domain, strconv.Itoa(int(svc.Spec.Ports[0].Port))))
		shDomain := enginefeatureseverestv1alpha1.SplitHorizonDomain{
			Domain:    domain,
			PrivateIP: svc.Spec.ClusterIP,
		}

		if a.DB.Spec.Proxy.Expose.Type == everestv1alpha1.ExposeTypeExternal {
			var ready bool
			if shDomain.PublicIP, ready = getServicePublicIP(ctx, svc); !ready {
				statusReady = false
			}
		}
		shStatus.Domains = append(shStatus.Domains, shDomain)
	}

	slices.SortFunc(shStatus.Domains, func(a, b enginefeatureseverestv1alpha1.SplitHorizonDomain) int {
		return strings.Compare(a.Domain, b.Domain)
	})

	slices.SortFunc(connHosts, strings.Compare)

	shStatus.Host = strings.Join(connHosts, ",")

	return shStatus, statusReady, nil
}

// getServicePublicIP retrieves the public IP address of the given service.
// It returns the public IP address and a boolean indicating whether the status is ready.
func getServicePublicIP(ctx context.Context, svc *corev1.Service) (string, bool) {
	logger := log.FromContext(ctx)

	if svc.Spec.Type != corev1.ServiceTypeLoadBalancer {
		return "", false
	}

	if len(svc.Status.LoadBalancer.Ingress) == 0 {
		return publicIPPendingValue, false
	}

	if svc.Status.LoadBalancer.Ingress[0].IP != "" {
		return svc.Status.LoadBalancer.Ingress[0].IP, true
	}

	// publicIP may be empty for load-balancer ingress points that are DNS based.
	// In such case Hostname shall be set (typically AWS)
	publicHostname := svc.Status.LoadBalancer.Ingress[0].Hostname
	if publicHostname == "" {
		return publicIPPendingValue, false
	}

	// try to resolve DNS name to IP
	var publicIPAddrs []net.IP
	var err error
	if publicIPAddrs, err = net.LookupIP(publicHostname); err != nil {
		// Binding IP to domain takes some time, so just log a warning here.
		logger.Info(fmt.Sprintf("resolve LoadBalancer ingress hostname %s to IP: %v", publicHostname, err))
		return publicIPPendingValue, false
	}

	for _, ip := range publicIPAddrs {
		if ipStr := ip.String(); ipStr != "<nil>" {
			return ipStr, true
		}
	}

	return publicIPPendingValue, false
}

// ----------------- Helpers -----------------
// issueSplitHorizonCertificate generates server TLS certificate signed by the provided CA certificate and private key,
// with SANs for the provided hosts.
// It returns the generated server TLS certificate, private key in PEM format and error.
func issueSplitHorizonCertificate(caCert, caPrivKey []byte, hosts []string) (string, string, error) {
	caDecoded, _ := pem.Decode(caCert)
	ca, err := x509.ParseCertificate(caDecoded.Bytes)
	if err != nil {
		return "", "", fmt.Errorf("parse CA certificate: %w", err)
	}

	caPrivKeyDecoded, _ := pem.Decode(caPrivKey)
	caKey, err := x509.ParsePKCS1PrivateKey(caPrivKeyDecoded.Bytes)
	if err != nil {
		return "", "", fmt.Errorf("parse CA private key: %w", err)
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128) //nolint:mnd
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return "", "", fmt.Errorf("generate serial number for client: %w", err)
	}

	serverCertTemplate := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"PSMDB"},
		},
		NotBefore:             time.Now(),
		NotAfter:              validityNotAfter,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}, //nolint:mnd
		DNSNames:              hosts,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}
	// Create server certificate private key
	certPrivKey, err := rsa.GenerateKey(rand.Reader, 2048) //nolint:mnd
	if err != nil {
		return "", "", fmt.Errorf("generate client key: %w", err)
	}

	// Create and sign server certificate with CA
	serverCertBytes, err := x509.CreateCertificate(rand.Reader, &serverCertTemplate, ca, &certPrivKey.PublicKey, caKey)
	if err != nil {
		return "", "", fmt.Errorf("generate server certificate: %w", err)
	}
	serverCertPem := &bytes.Buffer{}
	err = pem.Encode(serverCertPem, &pem.Block{Type: "CERTIFICATE", Bytes: serverCertBytes})
	if err != nil {
		return "", "", fmt.Errorf("encode server certificate: %w", err)
	}

	serverKeyPem := &bytes.Buffer{}
	block := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(certPrivKey)}
	err = pem.Encode(serverKeyPem, block)
	if err != nil {
		return "", "", fmt.Errorf("encode RSA private key: %w", err)
	}

	return serverCertPem.String(), serverKeyPem.String(), nil
}
