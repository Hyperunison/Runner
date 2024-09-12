This is stateless agent to run nextflow command or transfer debug data from cluster to hyperunison web platform

This utility orchestrates the execution of bash commands on a remote high performance compute (HPC) cluster. The following information is exhcanged with the remote HPC cluster is described in the Types of tasks section. 

# Architecture
 - The utility serves as a communication device between the Hyperunison platform and a cluster inside a virtual private network (VPN).
 - The utility connects the Hyperunison platform and the remote HPC cluster, as other network connections may be restricted by a firewall. Communication between the Hyperunison server and the utility installed in the remote HPC environment is facilitated using REST via an https connection. 
 - This utility orchestrates the running of nextflow pipelines built by Hyperunison on a remote HPC cluster. 
 - If further downstream analysis is requested by the client, Hyperunison nextflow pipelines upload data processing results into an AWS S3 bucket to make pipelie outputs available for further downstream analysis. Credentials to access the AWS S3 bucket as well as necessary path information are provided as parameters by the Hyperunison platform
 - The utility never export any private data from VPN, as it executes only SQL query like `SELECT COUNT(distinct patient_id), a, b, .. FROM .. GROUP BY a, b, .. HAVNIG COUNT(distinct patient_id) > N`. So any set of properties will not identify any person in biobank

## Logs
 - Documentation of all remote actions, like executed bash commands, and http requests
 - Logs are written to stdout by default
 - Elasticsearch support

# Installation

## configuration

## Using docker-compose
 - Copy config-dist.yaml to config.yaml
 - Update config.yaml to your needs. Properties are described in the config.yaml 
 - Run `docker-compose up --build`
After this you will see logs in your terminal

### AWS
 - generate credentials and place them into Resources/.aws folder. Folder should contain 2 files: `config` and `credentials`. 

### Kubernetes (k8s)
 - If you use k8s inside AWS, follow AWS instructions 
 - Copy k8s config to Resources/.kube folder. It should be a single file named: `config`
 - More inforation on k8s config file can be found [here](https://kubernetes.io/docs/concepts/configuration/organize-cluster-access-kubeconfig/)

## Agent configuration
 - config file is `config.yaml`

# Development
## Update API generated code (SDK)
```
$ bash update-api.sh
```

## Local env setup
```
$ make up
$ make sh
```

### Api update
```
$ make up-api
```


## Deploy on host
```
$ make deploy
```
