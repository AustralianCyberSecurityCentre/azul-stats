# Integration test Cluster

The docker compose file is used for creating a local test cluster for integration tests.

If you want to test Opensearch integration it is recommended you use the `azul-metastore` demo-cluster.

Ensuring that the password used for accessing Opensearch match.

## Startup

To start the cluster simply use the command  `docker compose up`.

This will give you a redis minio and kafka instance.

For azure storage you will need an azure cloud account and to setup a `storage account`.
