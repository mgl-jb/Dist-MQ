#!/usr/bin/env bash
# Build the broker image, push it, and deploy the Bicep template.
#
# Usage: NAME=distmq RESOURCE_GROUP=distmq-rg REGISTRY=myacr.azurecr.io ./infra/deploy.sh
set -euo pipefail

NAME="${NAME:?set NAME, e.g. distmq}"
RESOURCE_GROUP="${RESOURCE_GROUP:?set RESOURCE_GROUP}"
REGISTRY="${REGISTRY:?set REGISTRY, e.g. myacr.azurecr.io}"
LOCATION="${LOCATION:-westeurope}"
TAG="${TAG:-$(git rev-parse --short HEAD)}"
IMAGE="${REGISTRY}/distmq-broker:${TAG}"

echo "building ${IMAGE}"
docker build -t "${IMAGE}" .
docker push "${IMAGE}"

echo "deploying to ${RESOURCE_GROUP}"
az deployment group create \
  --resource-group "${RESOURCE_GROUP}" \
  --template-file infra/main.bicep \
  --parameters name="${NAME}" location="${LOCATION}" image="${IMAGE}" \
  --query 'properties.outputs' \
  --output json
