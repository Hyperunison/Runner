This is stateless agent to run nextflow command or transfer debug data from cluster to hyperunison web platform

This utility orchestrates the execution of bash commands on a remote high performance compute (HPC) cluster. The following information is exhcanged with the remote HPC cluster is described in the Types of tasks section. 

# Architecture
 - The utility serves as a communication device between the Hyperunison platform and a cluster inside a virtual private network (VPN).
 - The utility connects the Hyperunison platform and the remote HPC cluster, as other network connections may be restricted by a firewall. Communication between the Hyperunison server and the utility installed in the remote HPC environment is facilitated using REST via an https connection. 
 - This utility orchestrates the running of nextflow pipelines built by Hyperunison on a remote HPC cluster. 
 - If further downstream analysis is requested by the client, Hyperunison nextflow pipelines upload data processing results into an AWS S3 bucket to make pipelie outputs available for further downstream analysis. Credentials to access the AWS S3 bucket as well as necessary path information are provided as parameters by the Hyperunison platform

# Technical Specifications
## Code structure
- /Resources/ - Contains credentials, dockerfiles, and, if applicable, further parameter files
- /src/Adapters/ - Contains adapter applications for different cluster types. Now supporting k8s only
- /src/auto/ - Software development kit (SDK) to facilitate communication with the Hyperunison platform. This folder is autogenerated. Do not modify it manually! It is generated by the openAI [Swagger documentation tool](https://github.com/OpenAPITools/openapi-generator-cli). API specifications can be reviewed in [this file](https://api.genobase.pro/api/agent/doc)
- /src/Message/ - Documents commands (see types of tasks section) Hyperunison platform
- The utility needs to be installed within the client's VPN network. The utility can be installed anywhere within the VPN, but requieres a network connection to the HPC cluster. 

## Types of tasks
 - Run nextflow command (NextflowRun). Parameters:
   - nextflow_code (string) - (will be uploaded to cluster to seperate folder)
   - run_id (int) - in hyperunison platform for sending run status
   - dataset_id (int) - in hyperunison platform for sending run status
   - command - (string) nextflow command to run
   - input_data (array) - a list of objects with metadata and links to fastq files. It is used as input to nextflow
   - aws_id, aws_key, aws_s3_path (string) - AWS credentials and S3 to upload results of nextflow
 - Get process logs (GetProcessLogs). Ask last log lines from process (pod in k8s or similar entity in another clusters) Parameters:
   - process_id (string)
   - reply_channel (string) - where to send response
   - lines_limit (int) - how many lines to process
 - Kill Job (KillJob). Terminates a nextflow process in case it is no longer required. Parameters:
   - run_id (int) - what process to kill
   - channel (string) - where to send response

## Logs
 - Documentation of all remote actions, like executed bash commands, and http requests
 - Logs are written to stdout by default

# Installation

## Using docker-compose
```commandline
$ docker-compose up -d
```

## Using singularity: 
//todo

## Cluster configuration

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
