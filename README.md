# Installation

```commandline
docker-compose up -d
```

## Cluster configuration

### AWS
 - generate credentials and put them to Resources/.aws folder. Folder should contain 2 files: `config` and `credentials`

### k8s
 - If you use k8s inside AWS, follow AWS instructions first
 - Copy k8s config to Resources/.kube folder. It should be one file: `config`
 - More about k8s config file you may read here: https://kubernetes.io/docs/concepts/configuration/organize-cluster-access-kubeconfig/

## Agent configuration
 - config file is `config.yaml`
