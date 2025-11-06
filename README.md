# Azul Stats

Collects statistics from azul infrastructure and hosts them to be scraped by prometheus.

Infrastructure that can be scraped includes kafka and Opensearch.

## Installation

To configure azul-stats you need to set the appropriate environment variables as dictated in the settings.py file.

If you are using the helm install refer to `azul-app` values.yaml file.

```bash
pip install azul-stats
```

## Usage

```bash
azul-stats
```

## Stats collection overview

All stats are collected by making queries to the relevant piece of infrastructures API.

The stats collected from these queries include how long they took and whether or not they were successful.

This allows the prometheus metrics to show how slow the infrastructure is and indicates if there is a problem with the infrastructure.

It also allows for status checks, is the infrastructure actually up or is it down?

## Opensearch Stats

Opensearch stats are determined by performing the following actions:

- Authenticate to Opensearch
- Create an index in Opensearch.
- Index a document into Opensearch
- Run a standard Opensearch query to find the indexed document.
- Run an aggregation search against opensearch.

Note - deletes index after each run.

## Kafka Stats

Kafka stats are determined by performing the following actions:

- Can the kafka broker be contacted.
- Can a test topic be created.
- Can data be produced to a topic.
- Can data be consumed from the topic.

Note - deletes topic after each run.

## Running tests

To run integration tests you will need to startup a development cluster.
Refer to the instructions in `demo-cluster/readme.md` for details.

Once the cluster is started you can run the integration tests as normal using `pytest`
