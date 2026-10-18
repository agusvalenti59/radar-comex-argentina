terraform {
  required_version = ">= 1.6"
  required_providers {
    azurerm    = { source = "hashicorp/azurerm", version = "~> 4.10" }
    azuread    = { source = "hashicorp/azuread", version = "~> 3.0" }
    databricks = { source = "databricks/databricks", version = "~> 1.60" }
  }
  backend "azurerm" {} # ver envs/<env>.backend.hcl
}

provider "azurerm" {
  features {
    key_vault { purge_soft_delete_on_destroy = false }
  }
  subscription_id = var.subscription_id
}

provider "databricks" {
  alias                       = "workspace"
  host                        = module.databricks.workspace_url
  azure_workspace_resource_id = module.databricks.workspace_id
}
