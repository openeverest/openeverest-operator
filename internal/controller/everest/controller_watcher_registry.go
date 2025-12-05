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

// Package controllers contains a set of controllers for everest
package controllers

import (
	"sync"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// controllerWatcherRegistry is a wrapper around controller.Controller that provides a way to
// store and keep track of the sources that have been added to the controller.
type controllerWatcherRegistry struct {
	controller.Controller
	store sync.Map
	log   logr.Logger
}

func newControllerWatcherRegistry(log logr.Logger, c controller.Controller) *controllerWatcherRegistry {
	return &controllerWatcherRegistry{
		Controller: c,
		log:        log,
	}
}

// addWatchers adds the provided sources to the controller's watch and stores the name of the sources in a map to avoid adding them again.
func (c *controllerWatcherRegistry) addWatchers(
	name string,
	sources ...source.Source,
) error {
	_, ok := c.store.Load(name)
	if ok {
		return nil // watcher group already exists with this name, so skip.
	}
	for _, src := range sources {
		if err := c.Controller.Watch(src); err != nil {
			return err
		}
	}
	c.log.Info("Added watchers", "name", name)
	c.store.Store(name, struct{}{})
	return nil
}
