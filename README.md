## k8s

### Prerequisites

- kubectl
- minikube
- helm

### Setup

#### Load images

```shell
for img in mail-sender tx-consumer tx-producer tx-validator; do
  minikube image load $img:0.0.1
done
```

#### Install cloudnative-pg operator

```shell
helm upgrade --install cnpg \
  oci://ghcr.io/cloudnative-pg/charts/cloudnative-pg \
  --version 0.29.0 \
  --namespace cnpg-system \
  --create-namespace
```

#### Install strimzi operator

```shell
kubectl create namespace saga-go

helm upgrade --install strimzi-cluster-operator \
  oci://quay.io/strimzi-helm/strimzi-kafka-operator \
  --version 1.2.0 \
  --namespace strimzi-system \
  --create-namespace \
  --set 'watchNamespaces={saga-go}'
```

#### Deploy project

```shell
kubectl apply -R -f examples/deploy/k8s/
```

#### port forward UIs

##### Kafka UI

```shell
kubectl port-forward -n saga-go service/kafka-ui 8080:8080
```

##### MailHog UI

```shell
kubectl port-forward -n saga-go service/mailhog 8025:8025
```
