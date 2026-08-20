# ADR 0012: Deploy to Azure Container Apps via Bicep with managed identity

**Status:** Accepted

## Context

The broker is a stateless-ish container: state lives in Storage, and any replica
can take any partition. It needs horizontal scale, rolling revisions, and
credential-free access to Storage.

## Decision

Ship a Dockerfile and Bicep that provisions a Storage account, a Container Apps
environment and app, a user-assigned managed identity with
`Storage Blob Data Contributor` + `Storage Table Data Contributor`, and Log
Analytics. No connection strings in configuration.

## Consequences

- Scaling out is a replica count change; new replicas join, take leases and
  rebalance automatically.
- Managed identity means no secrets to rotate or leak.
- Container Apps gives less control over placement than AKS; partition ownership
  is decided by our own lease protocol anyway, so placement does not matter.
- Bicep is Azure-only; a Terraform port would be additive.

## Alternatives rejected

- **AKS** — more control, much more infrastructure to own for no benefit here.
- **App Service** — weaker container and scaling story for this workload.
