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

package pg

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/AlekSi/pointer"
	"github.com/go-ini/ini"
	crunchyv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/postgres-operator.crunchydata.com/v1beta1"
	corev1 "k8s.io/api/core/v1"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/controller/everest/common"
)

type pgReposReconciler struct {
	pgBackRestGlobal              map[string]string
	pgBackRestSecretIni           *ini.File
	backupStoragesInRepos         map[string]struct{}
	backupSchedulesToBeReconciled *list.List
	backupSchedulesReconciled     *list.List
	availableRepoNames            []string
	reposReconciled               *list.List
	reposToBeReconciled           *list.List
}

func newPGReposReconciler(oldRepos []crunchyv1beta1.PGBackRestRepo, backupSchedules []everestv1alpha1.BackupSchedule) (*pgReposReconciler, error) {
	iniFile, err := ini.Load([]byte{})
	if err != nil {
		return nil, errors.New("failed to initialize PGBackrest secret data")
	}
	reposToBeReconciled := list.New()
	for _, repo := range oldRepos {
		reposToBeReconciled.PushBack(repo)
	}
	backupSchedulesToBeReconciled := list.New()
	for _, backupSchedule := range backupSchedules {
		if !backupSchedule.Enabled {
			continue
		}
		backupSchedulesToBeReconciled.PushBack(backupSchedule)
	}

	const (
		maxStorages                = 4
		maxPGBackrestOptionsNumber = 150 // the full potential list https://pgbackrest.org/configuration.html
	)

	pgBackRestGlobal := make(map[string]string, maxPGBackrestOptionsNumber*maxStorages)

	return &pgReposReconciler{
		pgBackRestGlobal:              pgBackRestGlobal,
		pgBackRestSecretIni:           iniFile,
		backupStoragesInRepos:         make(map[string]struct{}, maxStorages),
		reposToBeReconciled:           reposToBeReconciled,
		reposReconciled:               list.New(),
		backupSchedulesReconciled:     list.New(),
		backupSchedulesToBeReconciled: backupSchedulesToBeReconciled,
		availableRepoNames:            []string{"repo1", "repo2", "repo3", "repo4"},
	}, nil
}

func (p *pgReposReconciler) addRepoToPGGlobal(
	verifyTLS *bool,
	repoName string,
	forcePathStyle *bool,
	retentionCopies *int32,
	db *everestv1alpha1.DatabaseCluster,
) {
	p.pgBackRestGlobal[fmt.Sprintf(pgBackRestPathTmpl, repoName)] = "/" + common.BackupStoragePrefix(db)
	if retentionCopies != nil {
		p.pgBackRestGlobal[fmt.Sprintf(pgBackRestRetentionTmpl, repoName)] = strconv.Itoa(getPGRetentionCopies(*retentionCopies))
	}
	if verify := pointer.Get(verifyTLS); !verify {
		// See: https://pgbackrest.org/configuration.html#section-repository/option-repo-storage-verify-tls
		p.pgBackRestGlobal[fmt.Sprintf(pgBackRestStorageVerifyTmpl, repoName)] = "n"
	}
	if forceStyle := pointer.Get(forcePathStyle); !forceStyle {
		// See: https://pgbackrest.org/configuration.html#section-repository/option-repo-s3-uri-style
		p.pgBackRestGlobal[fmt.Sprintf(pgBackRestStorageForcePathTmpl, repoName)] = pgBackRestStoragePathStyle
	}
}

func (p *pgReposReconciler) extractFirstAvailableRepoName() (string, error) {
	if len(p.availableRepoNames) == 0 {
		return "", errors.New("exceeded max number of repos")
	}
	repoName := p.availableRepoNames[0]
	p.removeAvailableRepoName(repoName)
	return repoName, nil
}

func (p *pgReposReconciler) removeAvailableRepoName(n string) {
	for i, name := range p.availableRepoNames {
		if name == n {
			p.availableRepoNames = removeString(p.availableRepoNames, i)
			return
		}
	}
}

func removeString(s []string, i int) []string {
	return append(s[:i], s[i+1:]...)
}

// Update repos based on the existing backups.
func (p *pgReposReconciler) reconcileBackups(
	backups []everestv1alpha1.DatabaseClusterBackup,
	backupStorages map[string]everestv1alpha1.BackupStorage,
	backupStoragesSecrets map[string]*corev1.Secret,
	db *everestv1alpha1.DatabaseCluster,
) error {
	// Add on-demand backups whose backup storage doesn't have a schedule
	// defined
	// XXX some of these backups might be fake, see the XXX comment in the
	// reconcilePGBackupsSpec function for more context. Be very careful if you
	// decide to use fields other that the Spec.BackupStorageName.
	for _, backup := range backups {
		// Backup storage already in repos, skip
		if _, ok := p.backupStoragesInRepos[backup.Spec.BackupStorageName]; ok {
			continue
		}

		backupStorage, ok := backupStorages[backup.Spec.BackupStorageName]
		if !ok {
			return fmt.Errorf("unknown backup storage %s", backup.Spec.BackupStorageName)
		}

		// try to find the appropriate repo in the existing repos
		var repoName string
		var erNext *list.Element
		for er := p.reposToBeReconciled.Front(); er != nil; er = erNext {
			// Save the next element because we might remove the current one
			erNext = er.Next()

			repo, ok := er.Value.(crunchyv1beta1.PGBackRestRepo)
			if !ok {
				return fmt.Errorf("failed to cast repo %v", er.Value)
			}
			repoBackupStorageName := backupStorageNameFromRepo(backupStorages, repo, db.Namespace)
			if backup.Spec.BackupStorageName != repoBackupStorageName {
				continue
			}
			repoName = repo.Name
			p.removeAvailableRepoName(repo.Name)
			// remove the repo from the reconciled list bc it will be re-added below with the schedule string
			p.reposReconciled.Remove(er)
			break
		}
		// if there is no repo for this backup - create a new one
		if repoName == "" {
			var err error
			repoName, err = p.extractFirstAvailableRepoName()
			if err != nil {
				return err
			}
		}

		repo, err := genPGBackrestRepo(repoName, backupStorage.Spec, nil)
		if err != nil {
			return err
		}
		p.reposReconciled.PushBack(repo)
		// Keep track of backup storages which are already in use by a repo
		p.backupStoragesInRepos[backup.Spec.BackupStorageName] = struct{}{}

		p.addRepoToPGGlobal(backupStorage.Spec.VerifyTLS, repo.Name, backupStorages[repo.Name].Spec.ForcePathStyle, nil, db)
		err = updatePGIni(p.pgBackRestSecretIni, backupStoragesSecrets[backup.Spec.BackupStorageName], repo)
		if err != nil {
			return errors.Join(err, errors.New("failed to add backup storage credentials to PGBackrest secret data"))
		}
	}
	return nil
}

// Update the schedules of the repos which already exist but have the wrong
// schedule and move them to the reconciled list.
func (p *pgReposReconciler) reconcileExistingSchedules(
	backupStorages map[string]everestv1alpha1.BackupStorage,
	backupStoragesSecrets map[string]*corev1.Secret,
	db *everestv1alpha1.DatabaseCluster,
) error {
	// Move repos with set schedules which are already correct to the
	// reconciled list
	var ebNext *list.Element
	var erNext *list.Element
	for eb := p.backupSchedulesToBeReconciled.Front(); eb != nil; eb = ebNext {
		// Save the next element because we might remove the current one
		ebNext = eb.Next()

		backupSchedule, ok := eb.Value.(everestv1alpha1.BackupSchedule)
		if !ok {
			return fmt.Errorf("failed to cast backup schedule %v", eb.Value)
		}
		for er := p.reposToBeReconciled.Front(); er != nil; er = erNext {
			// Save the next element because we might remove the current one
			erNext = er.Next()

			repo, ok := er.Value.(crunchyv1beta1.PGBackRestRepo)
			if !ok {
				return fmt.Errorf("failed to cast repo %v", er.Value)
			}
			repoBackupStorageName := backupStorageNameFromRepo(backupStorages, repo, db.Namespace)
			if backupSchedule.BackupStorageName != repoBackupStorageName ||
				repo.BackupSchedules == nil ||
				repo.BackupSchedules.Full == nil ||
				backupSchedule.Schedule != *repo.BackupSchedules.Full {
				continue
			}

			p.reposReconciled.PushBack(repo)
			p.reposToBeReconciled.Remove(er)
			p.backupSchedulesReconciled.PushBack(backupSchedule)
			p.backupSchedulesToBeReconciled.Remove(eb)
			p.removeAvailableRepoName(repo.Name)

			// Keep track of backup storages which are already in use by a repo
			p.backupStoragesInRepos[backupSchedule.BackupStorageName] = struct{}{}

			p.addRepoToPGGlobal(backupStorages[repo.Name].Spec.VerifyTLS, repo.Name, backupStorages[repo.Name].Spec.ForcePathStyle, &backupSchedule.RetentionCopies, db)
			err := updatePGIni(p.pgBackRestSecretIni, backupStoragesSecrets[backupSchedule.BackupStorageName], repo)
			if err != nil {
				return errors.Join(err, errors.New("failed to add backup storage credentials to PGBackrest secret data"))
			}
			break
		}
	}
	return nil
}

// Update repos based on the schedules.
func (p *pgReposReconciler) reconcileRepos(
	backupStorages map[string]everestv1alpha1.BackupStorage,
	backupStoragesSecrets map[string]*corev1.Secret,
	db *everestv1alpha1.DatabaseCluster,
) error {
	var ebNext *list.Element
	var erNext *list.Element
	for er := p.reposToBeReconciled.Front(); er != nil; er = erNext {
		// Save the next element because we might remove the current one
		erNext = er.Next()

		repo, ok := er.Value.(crunchyv1beta1.PGBackRestRepo)
		if !ok {
			return fmt.Errorf("failed to cast repo %v", er.Value)
		}
		repoBackupStorageName := backupStorageNameFromRepo(backupStorages, repo, db.Namespace)
		for eb := p.backupSchedulesToBeReconciled.Front(); eb != nil; eb = ebNext {
			// Save the next element because we might remove the current one
			ebNext = eb.Next()

			backupSchedule, ok := eb.Value.(everestv1alpha1.BackupSchedule)
			if !ok {
				return fmt.Errorf("failed to cast backup schedule %v", eb.Value)
			}
			if backupSchedule.BackupStorageName == repoBackupStorageName {
				repo.BackupSchedules = &crunchyv1beta1.PGBackRestBackupSchedules{
					Full: &backupSchedule.Schedule,
				}

				p.reposReconciled.PushBack(repo)
				p.reposToBeReconciled.Remove(er)
				p.backupSchedulesReconciled.PushBack(backupSchedule)
				p.backupSchedulesToBeReconciled.Remove(eb)
				p.removeAvailableRepoName(repo.Name)

				// Keep track of backup storages which are already in use by a repo
				p.backupStoragesInRepos[backupSchedule.BackupStorageName] = struct{}{}

				p.addRepoToPGGlobal(backupStorages[repo.Name].Spec.VerifyTLS,
					repo.Name, backupStorages[repo.Name].Spec.ForcePathStyle,
					&backupSchedule.RetentionCopies, db,
				)
				err := updatePGIni(p.pgBackRestSecretIni, backupStoragesSecrets[backupSchedule.BackupStorageName], repo)
				if err != nil {
					return errors.Join(err, errors.New("failed to add backup storage credentials to PGBackrest secret data"))
				}
				break
			}
		}
	}
	return nil
}

func (p *pgReposReconciler) addNewSchedules( //nolint:gocognit
	backupStorages map[string]everestv1alpha1.BackupStorage,
	backupStoragesSecrets map[string]*corev1.Secret,
	db *everestv1alpha1.DatabaseCluster,
) error {
	// Add new backup schedules
	for eb := p.backupSchedulesToBeReconciled.Front(); eb != nil; eb = eb.Next() {
		backupSchedule, ok := eb.Value.(everestv1alpha1.BackupSchedule)
		if !ok {
			return fmt.Errorf("failed to cast backup schedule %v", eb.Value)
		}
		backupStorage, ok := backupStorages[backupSchedule.BackupStorageName]
		if !ok {
			return fmt.Errorf("unknown backup storage %s", backupSchedule.BackupStorageName)
		}

		// try to reuse the repos already used by backups
		var repoName string
		var erNext *list.Element
		for er := p.reposToBeReconciled.Front(); er != nil; er = erNext {
			// Save the next element because we might remove the current one
			erNext = er.Next()

			repo, ok := er.Value.(crunchyv1beta1.PGBackRestRepo)
			if !ok {
				return fmt.Errorf("failed to cast repo %v", er.Value)
			}
			repoBackupStorageName := backupStorageNameFromRepo(backupStorages, repo, db.Namespace)
			if backupSchedule.BackupStorageName != repoBackupStorageName ||
				repo.BackupSchedules != nil {
				continue
			}
			repoName = repo.Name
			p.removeAvailableRepoName(repo.Name)
			break
		}
		// check the freshly reconciled repos if there is already such a repo there
		for er := p.reposReconciled.Front(); er != nil; er = erNext {
			// Save the next element because we might remove the current one
			erNext = er.Next()

			repo, ok := er.Value.(crunchyv1beta1.PGBackRestRepo)
			if !ok {
				return fmt.Errorf("failed to cast repo %v", er.Value)
			}
			repoBackupStorageName := backupStorageNameFromRepo(backupStorages, repo, db.Namespace)
			if backupSchedule.BackupStorageName != repoBackupStorageName ||
				repo.BackupSchedules != nil {
				continue
			}
			repoName = repo.Name
			p.removeAvailableRepoName(repo.Name)
			// remove the repo from the reconciled list bc we need to update it by adding a schedule
			p.reposReconciled.Remove(er)
			break
		}
		// if no existing repos can be reused - create a new one
		if repoName == "" {
			var err error
			repoName, err = p.extractFirstAvailableRepoName()
			if err != nil {
				return err
			}
		}

		repo, err := genPGBackrestRepo(repoName, backupStorage.Spec, &backupSchedule.Schedule)
		if err != nil {
			return err
		}
		p.reposReconciled.PushBack(repo)

		// Keep track of backup storages which are already in use by a repo
		p.backupStoragesInRepos[backupSchedule.BackupStorageName] = struct{}{}

		p.addRepoToPGGlobal(backupStorage.Spec.VerifyTLS, repo.Name, backupStorages[repo.Name].Spec.ForcePathStyle, &backupSchedule.RetentionCopies, db)
		err = updatePGIni(p.pgBackRestSecretIni, backupStoragesSecrets[backupSchedule.BackupStorageName], repo)
		if err != nil {
			return errors.Join(err, errors.New("failed to add backup storage credentials to PGBackrest secret data"))
		}
	}
	return nil
}

func (p *pgReposReconciler) pgBackRestIniBytes() ([]byte, error) {
	pgBackrestSecretBuf := new(bytes.Buffer)
	if _, err := p.pgBackRestSecretIni.WriteTo(pgBackrestSecretBuf); err != nil {
		return []byte{}, errors.New("failed to write PGBackrest secret data")
	}
	return pgBackrestSecretBuf.Bytes(), nil
}

func (p *pgReposReconciler) sortRepos() ([]crunchyv1beta1.PGBackRestRepo, error) {
	var newRepos []crunchyv1beta1.PGBackRestRepo
	// Add the reconciled repos to the list of repos
	for e := p.reposReconciled.Front(); e != nil; e = e.Next() {
		repo, ok := e.Value.(crunchyv1beta1.PGBackRestRepo)
		if !ok {
			return []crunchyv1beta1.PGBackRestRepo{}, fmt.Errorf("failed to cast repo %v", e.Value)
		}
		newRepos = append(newRepos, repo)
	}
	sortByName(newRepos)
	return newRepos, nil
}

func sortByName(repos []crunchyv1beta1.PGBackRestRepo) {
	sort.Slice(repos, func(i, j int) bool {
		return repos[i].Name < repos[j].Name
	})
}

func updatePGIni(
	pgBackRestSecretIni *ini.File,
	secret *corev1.Secret,
	repo crunchyv1beta1.PGBackRestRepo,
) error {
	sType, err := backupStorageTypeFromBackrestRepo(repo)
	if err != nil {
		return err
	}
	return addBackupStorageCredentialsToPGBackrestSecretIni(sType, pgBackRestSecretIni, repo.Name, secret)
}

func backupStorageTypeFromBackrestRepo(repo crunchyv1beta1.PGBackRestRepo) (everestv1alpha1.BackupStorageType, error) {
	if repo.S3 != nil {
		return everestv1alpha1.BackupStorageTypeS3, nil
	}

	if repo.Azure != nil {
		return everestv1alpha1.BackupStorageTypeAzure, nil
	}

	return "", errors.New("could not determine backup storage type from repo")
}
