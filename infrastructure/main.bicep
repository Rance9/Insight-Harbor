// Insight Harbor — Core Infrastructure (Phase 1 Deploy)
// Resource Group: insight-harbor-rg (East US 2)
// Subscription:   Visual Studio Enterprise (bmiddendorf@gmail.com)
//
// Resources provisioned in this file:
//   1. ADLS Gen2 Storage Account    — Bronze & Silver data layers
//   2. Azure Automation Account     — Scheduled ingestion runbooks
//
// NOTE: Functions App + Static Web App are in main-dashboard.bicep (Phase 7).
// Those require Dynamic VM quota in the subscription. To deploy Phase 7:
//   1. Request quota increase: https://portal.azure.com/#view/Microsoft_Azure_Capacity
//   2. Request type: "Compute-VM (cores-vCPUs) subscription limit increases"
//   3. Location: East US 2, SKU: "Dynamic" (Consumption plan), New limit: 1
//   4. Then deploy: az deployment group create --template-file infrastructure/main-dashboard.bicep
//
// Deploy this file (core resources only):
//   az deployment group create \
//     --resource-group insight-harbor-rg \
//     --template-file infrastructure/main.bicep \
//     --parameters @infrastructure/parameters.json

@description('Azure region for all resources.')
param location string = 'eastus2'

@description('Unique suffix appended to storage account name (3-5 lowercase chars/digits).')
@minLength(3)
@maxLength(5)
param uniqueSuffix string

@description('Name of the ADLS Gen2 storage account for Bronze/Silver data layers.')
param adlsAccountName string = 'ihstorage${uniqueSuffix}'

@description('Name of the ADLS Gen2 container for Insight Harbor data.')
param adlsContainerName string = 'insight-harbor'

@description('Name of the Azure Automation Account.')
param automationAccountName string = 'ih-automation'

// ─────────────────────────────────────────────────────────────────────────────
// 1. ADLS Gen2 Storage Account
// ─────────────────────────────────────────────────────────────────────────────

resource adlsAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: adlsAccountName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    isHnsEnabled: true            // Required for ADLS Gen2 hierarchical namespace
    accessTier: 'Cool'            // Cost-optimized for batch (infrequent) access
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    publicNetworkAccess: 'Enabled'
  }
}

resource adlsBlobService 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: adlsAccount
  name: 'default'
}

resource adlsContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: adlsBlobService
  name: adlsContainerName
  properties: {
    publicAccess: 'None'
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Azure Automation Account
// ─────────────────────────────────────────────────────────────────────────────

resource automationAccount 'Microsoft.Automation/automationAccounts@2023-11-01' = {
  name: automationAccountName
  location: location
  identity: {
    type: 'SystemAssigned'      // Managed identity for future Key Vault integration
  }
  properties: {
    sku: {
      name: 'Free'              // Free tier: 500 min/month — sufficient for PoC daily runs
    }
    publicNetworkAccess: true
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Outputs — copy these into insight-harbor-config.json after deployment
// ─────────────────────────────────────────────────────────────────────────────

output adlsAccountName string = adlsAccount.name
output adlsContainerName string = adlsContainerName
output adlsPrimaryEndpoint string = adlsAccount.properties.primaryEndpoints.blob
output adlsResourceId string = adlsAccount.id
output automationAccountName string = automationAccount.name
output automationManagedIdentityPrincipalId string = automationAccount.identity.principalId
