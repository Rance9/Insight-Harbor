// Insight Harbor — Dashboard Infrastructure (Phase 7 Deploy)
// Resource Group: insight-harbor-rg (East US 2) — RG location
// Dashboard resources deployed to: Central US  (Dynamic VM quota available)
// Subscription:   Visual Studio Enterprise (bmiddendorf@gmail.com)
//
// Resources provisioned in this file:
//   1. Azure Static Web App         — HTML dashboard frontend (Free tier)
//   2. Azure Functions App          — REST API for dashboard (Consumption plan / Linux)
//   3. Azure Functions Storage Acct — Internal Functions storage (NOT the data lake)
//
// NOTE: Dashboard resources are in Central US because Dynamic VM quota = 0 in
//       East US 2 for this subscription. The Functions app connects cross-region
//       to ADLS Gen2 (ihstoragepoc01) in East US 2 via connection string.
//
// Deploy command:
//   az deployment group create \
//     --resource-group insight-harbor-rg \
//     --template-file infrastructure/main-dashboard.bicep \
//     --parameters @infrastructure/parameters-dashboard.json

@description('Azure region for dashboard resources (Central US has Dynamic VM quota).')
param location string = 'centralus'

@description('Unique suffix for globally unique resource names (3-5 lowercase chars/digits).')
@minLength(3)
@maxLength(5)
param uniqueSuffix string

@description('Name of the ADLS Gen2 account (already deployed — referenced for app settings).')
param adlsAccountName string = 'ihstorage${uniqueSuffix}'

@description('Name of the ADLS Gen2 container.')
param adlsContainerName string = 'insight-harbor'

@description('Name of the Azure Static Web App.')
param staticWebAppName string = 'ih-dashboard'

@description('Name of the Azure Functions App.')
param functionAppName string = 'ih-api-${uniqueSuffix}'

@description('Name of the storage account used internally by Azure Functions.')
param functionStorageAccountName string = 'ihfuncstor${uniqueSuffix}'

// ─────────────────────────────────────────────────────────────────────────────
// 1. Azure Static Web App
// ─────────────────────────────────────────────────────────────────────────────

resource staticWebApp 'Microsoft.Web/staticSites@2023-01-01' = {
  name: staticWebAppName
  location: location
  sku: {
    name: 'Free'
    tier: 'Free'
  }
  properties: {
    stagingEnvironmentPolicy: 'Disabled'
    allowConfigFileUpdates: true
    enterpriseGradeCdnStatus: 'Disabled'
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Functions Internal Storage Account
// ─────────────────────────────────────────────────────────────────────────────

resource functionStorageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: functionStorageAccountName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    isHnsEnabled: false
    accessTier: 'Hot'
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Functions Consumption Plan + App
// ─────────────────────────────────────────────────────────────────────────────

resource functionAppPlan 'Microsoft.Web/serverfarms@2023-01-01' = {
  name: 'ih-consumption-plan'
  location: location
  kind: 'functionapp'
  sku: {
    name: 'Y1'
    tier: 'Dynamic'
  }
  properties: {
    reserved: true    // Linux
  }
}

resource functionApp 'Microsoft.Web/sites@2023-01-01' = {
  name: functionAppName
  location: location
  kind: 'functionapp,linux'
  properties: {
    serverFarmId: functionAppPlan.id
    httpsOnly: true
    siteConfig: {
      pythonVersion: '3.11'
      linuxFxVersion: 'Python|3.11'
      appSettings: [
        {
          name: 'AzureWebJobsStorage'
          value: 'DefaultEndpointsProtocol=https;AccountName=${functionStorageAccountName};AccountKey=${functionStorageAccount.listKeys().keys[0].value};EndpointSuffix=core.windows.net'
        }
        {
          name: 'FUNCTIONS_EXTENSION_VERSION'
          value: '~4'
        }
        {
          name: 'FUNCTIONS_WORKER_RUNTIME'
          value: 'python'
        }
        {
          name: 'WEBSITE_RUN_FROM_PACKAGE'
          value: '1'
        }
        {
          name: 'IH_ADLS_ACCOUNT_NAME'
          value: adlsAccountName
        }
        {
          name: 'IH_ADLS_CONTAINER'
          value: adlsContainerName
        }
      ]
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Outputs
// ─────────────────────────────────────────────────────────────────────────────

output staticWebAppUrl string = staticWebApp.properties.defaultHostname
output functionAppUrl string = 'https://${functionApp.properties.defaultHostName}'
output functionAppName string = functionApp.name
