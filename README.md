# OpenEverest Operator

![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/percona/everest-operator)
[![Go Report Card](https://goreportcard.com/badge/github.com/openeverest/openeverest-operator)](https://goreportcard.com/report/github.com/openeverest/openeverest-operator)

OpenEverest Operator is one of the core components of [OpenEverest](https://github.com/openeverest/openeverest), an open source cloud-native database platform that helps developers deploy code faster, scale deployments rapidly, and reduce database administration overhead while regaining control over their data, database configuration, and DBaaS costs.

OpenEverest Operator is a Kubernetes Operator responsible for managing the lifecycle of MySQL, MongoDB, and PostgreSQL databases. It leverages Kubernetes Operators for [MySQL](https://github.com/percona/percona-xtradb-cluster-operator), [MongoDB](https://github.com/percona/percona-server-mongodb-operator), and [PostgreSQL](https://github.com/percona/percona-postgresql-operator/) under the hood but provides a unified API and a single pane of glass for managing all three database types.

Ready to try out OpenEverest? Check the [Quickstart install](https://openeverest.io/documentation/current/quick-install.html) section for easy-to-follow steps.
