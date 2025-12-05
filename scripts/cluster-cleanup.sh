#!/bin/sh

kubectl delete db --all-namespaces --all --cascade=foreground --ignore-not-found=true || true
namespaces=$(kubectl get pxc -A -o jsonpath='{.items[*].metadata.namespace}');
for ns in $namespaces; do
    kubectl -n $ns get pxc -o name | xargs --no-run-if-empty -I{} kubectl patch -n $ns {} -p '{"metadata":{"finalizers":null}}' --type=merge;
done
namespaces=$(kubectl get psmdb -A -o jsonpath='{.items[*].metadata.namespace}');
for ns in $namespaces; do
    kubectl -n $ns get psmdb -o name | xargs --no-run-if-empty -I{} kubectl patch -n $ns {} -p '{"metadata":{"finalizers":null}}' --type=merge;
done
namespaces=$(kubectl get pg -A -o jsonpath='{.items[*].metadata.namespace}');
for ns in $namespaces; do
    kubectl -n $ns get pg -o name | xargs --no-run-if-empty -I{} kubectl patch -n $ns {} -p '{"metadata":{"finalizers":null}}' --type=merge;
done
namespaces=$(kubectl get db -A -o jsonpath='{.items[*].metadata.namespace}');
for ns in $namespaces; do
    kubectl -n $ns get db -o name | xargs --no-run-if-empty -I{} kubectl patch -n $ns {} -p '{"metadata":{"finalizers":null}}' --type=merge;
done
namespaces=$(kubectl get db -A -o jsonpath='{.items[*].metadata.namespace}');
for ns in $namespaces; do
    kubectl -n $ns delete -f ./test/testdata/minio --ignore-not-found || true;
done
kubectl delete pvc --all-namespaces --all --ignore-not-found=true || true
kubectl delete backupstorage --all-namespaces --all --ignore-not-found=true || true
kubectl get ns -o name | grep kuttl | xargs --no-run-if-empty kubectl delete || true
kubectl delete ns operators olm --ignore-not-found=true --wait=false || true
sleep 10
kubectl delete apiservice v1.packages.operators.coreos.com --ignore-not-found=true || true
kubectl get crd -o name | grep .coreos.com$ | xargs --no-run-if-empty kubectl delete || true
kubectl get crd -o name | grep .percona.com$ | xargs --no-run-if-empty kubectl delete || true
kubectl delete crd postgresclusters.postgres-operator.crunchydata.com --ignore-not-found=true || true
