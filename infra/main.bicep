// Dist-MQ on Azure Container Apps.
//
// The broker keeps nothing locally: partitions, logs and cluster state all live in the
// storage account, so replicas are interchangeable and scaling out is a replica count
// change. Access is by managed identity — there is no connection string anywhere in this
// template, and nothing to rotate.

targetScope = 'resourceGroup'

@description('Short name used to derive resource names.')
@minLength(3)
@maxLength(17)
param name string

@description('Location for every resource.')
param location string = resourceGroup().location

@description('Container image for the broker.')
param image string

@description('Number of broker replicas. Each one takes a share of the partitions.')
@minValue(1)
@maxValue(30)
param replicas int = 2

@description('Storage redundancy. ZRS survives a zone failure; LRS is cheaper.')
@allowed(['Standard_LRS', 'Standard_ZRS'])
param storageSku string = 'Standard_ZRS'

@description('Log Analytics retention in days.')
param retentionInDays int = 30

var storageName = toLower('${replace(name, '-', '')}stg')
var identityName = '${name}-identity'
var environmentName = '${name}-env'
var appName = '${name}-broker'
var workspaceName = '${name}-logs'

// Built-in role definitions. The broker reads and writes blobs and tables and needs
// nothing else — notably no management-plane rights over the account itself.
var blobDataContributor = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
)
var tableDataContributor = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3'
)

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: identityName
  location: location
}

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageName
  location: location
  sku: {
    name: storageSku
  }
  kind: 'StorageV2'
  properties: {
    // The broker authenticates with its identity; shared keys are an alternative path to
    // the same data and are switched off rather than merely unused.
    allowSharedKeyAccess: false
    allowBlobPublicAccess: false
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
  }
}

resource blobRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, identity.id, blobDataContributor)
  scope: storage
  properties: {
    principalId: identity.properties.principalId
    roleDefinitionId: blobDataContributor
    principalType: 'ServicePrincipal'
  }
}

resource tableRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, identity.id, tableDataContributor)
  scope: storage
  properties: {
    principalId: identity.properties.principalId
    roleDefinitionId: tableDataContributor
    principalType: 'ServicePrincipal'
  }
}

resource workspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: workspaceName
  location: location
  properties: {
    retentionInDays: retentionInDays
    sku: {
      name: 'PerGB2018'
    }
  }
}

resource environment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: environmentName
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: workspace.properties.customerId
        sharedKey: workspace.listKeys().primarySharedKey
      }
    }
  }
}

resource app 'Microsoft.App/containerApps@2024-03-01' = {
  name: appName
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  properties: {
    managedEnvironmentId: environment.id
    configuration: {
      ingress: {
        external: true
        // The gRPC data plane is the main ingress. It needs end-to-end HTTP/2: the edge
        // terminates TLS and forwards h2c to the container, and without this the data
        // plane falls back to HTTP/1.1 and every call fails with HTTP_1_1_REQUIRED.
        targetPort: 5001
        transport: 'http2'
        allowInsecure: false
        additionalPortMappings: [
          {
            // Administration, the HTTP data plane and the WebSocket bridge. A separate
            // port because a cleartext container port cannot serve both protocols.
            external: true
            targetPort: 5000
            exposedPort: 5000
          }
        ]
      }
    }
    template: {
      containers: [
        {
          name: 'broker'
          image: image
          resources: {
            cpu: json('1.0')
            memory: '2Gi'
          }
          env: [
            { name: 'DistMq__Storage', value: 'Azure' }
            { name: 'DistMq__BlobServiceUri', value: storage.properties.primaryEndpoints.blob }
            { name: 'DistMq__TableServiceUri', value: storage.properties.primaryEndpoints.table }
            { name: 'DistMq__Namespace', value: name }
            { name: 'DistMq__Cluster__Enabled', value: 'true' }
            // Identity for DefaultAzureCredential. With several identities assigned, the
            // client id is what disambiguates them.
            { name: 'AZURE_CLIENT_ID', value: identity.properties.clientId }
          ]
          probes: [
            {
              type: 'Liveness'
              httpGet: { path: '/health/live', port: 5000 }
              periodSeconds: 15
            }
            {
              type: 'Readiness'
              httpGet: { path: '/health/ready', port: 5000 }
              periodSeconds: 10
            }
          ]
        }
      ]
      scale: {
        // Fixed rather than autoscaled: every replica that joins or leaves moves
        // partitions, and each move costs a lease handover and a log replay. Scaling on
        // request rate would churn ownership for no benefit.
        minReplicas: replicas
        maxReplicas: replicas
      }
    }
  }
  dependsOn: [blobRole, tableRole]
}

output brokerGrpcFqdn string = app.properties.configuration.ingress.fqdn
output brokerHttpEndpoint string = 'https://${app.properties.configuration.ingress.fqdn}:5000'
output storageAccountName string = storage.name
output identityClientId string = identity.properties.clientId
