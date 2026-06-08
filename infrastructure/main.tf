# ============================================================
# Infrastructure as Code: Terraform configuration
# Deploys all Azure resources for the data pipeline
# ============================================================

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

provider "azurerm" {
  features {}
}

variable "resource_group_name" {
  default = "rg-hpe-data-pipeline"
}

variable "location" {
  default = "East US"
}

variable "environment" {
  default = "dev"
}

variable "project_name" {
  default = "hpe-o9"
}

# ============================================================
# Resource Group
# ============================================================
resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# ============================================================
# Azure Data Lake Storage Gen2
# ============================================================
resource "azurerm_storage_account" "adls" {
  name                     = "adls${var.project_name}${var.environment}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true  # Hierarchical namespace for ADLS Gen2

  tags = {
    Environment = var.environment
  }
}

# ADLS Containers
resource "azurerm_storage_data_lake_gen2_filesystem" "landing" {
  name               = "landing"
  storage_account_id = azurerm_storage_account.adls.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.adls.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.adls.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gold" {
  name               = "gold"
  storage_account_id = azurerm_storage_account.adls.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "archive" {
  name               = "archive"
  storage_account_id = azurerm_storage_account.adls.id
}

# ============================================================
# Azure SQL Database (Audit)
# ============================================================
resource "azurerm_mssql_server" "audit" {
  name                         = "sql-${var.project_name}-${var.environment}"
  resource_group_name          = azurerm_resource_group.main.name
  location                     = azurerm_resource_group.main.location
  version                      = "12.0"
  administrator_login          = "sqladmin"
  administrator_login_password = var.sql_admin_password

  tags = {
    Environment = var.environment
  }
}

variable "sql_admin_password" {
  sensitive = true
}

resource "azurerm_mssql_database" "audit_db" {
  name      = "audit-db"
  server_id = azurerm_mssql_server.audit.id
  sku_name  = "S1"

  tags = {
    Environment = var.environment
  }
}

# ============================================================
# Azure Key Vault
# ============================================================
data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "main" {
  name                = "kv-${var.project_name}-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    secret_permissions = ["Get", "List", "Set", "Delete"]
  }
}

# ============================================================
# Azure Databricks Workspace
# ============================================================
resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-${var.project_name}-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "standard"

  tags = {
    Environment = var.environment
  }
}

# ============================================================
# Azure Data Factory
# ============================================================
resource "azurerm_data_factory" "main" {
  name                = "adf-${var.project_name}-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  identity {
    type = "SystemAssigned"
  }

  tags = {
    Environment = var.environment
  }
}

# ============================================================
# Outputs
# ============================================================
output "adls_account_name" {
  value = azurerm_storage_account.adls.name
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.main.workspace_url
}

output "sql_server_fqdn" {
  value = azurerm_mssql_server.audit.fully_qualified_domain_name
}

output "data_factory_name" {
  value = azurerm_data_factory.main.name
}
