locals {
  name = "${var.project}-${var.env}"
  # Nombres de storage: sin guiones, <= 24 chars
  st_name = "st${var.project}${var.env == "prod" ? "prd" : var.env}"
  tags = merge(var.tags, {
    project     = "trade-intelligence"
    environment = var.env
    managed_by  = "terraform"
  })
}

resource "azurerm_resource_group" "this" {
  name     = "rg-${local.name}"
  location = var.location
  tags     = local.tags
}

module "monitoring" {
  source              = "./modules/monitoring"
  name                = local.name
  resource_group_name = azurerm_resource_group.this.name
  location            = var.location
  tags                = local.tags
}

module "storage" {
  source                     = "./modules/storage"
  name                       = local.st_name
  resource_group_name        = azurerm_resource_group.this.name
  location                   = var.location
  containers                 = ["landing", "lakehouse", "checkpoints"]
  replication                = var.env == "prod" ? "ZRS" : "LRS"
  log_analytics_workspace_id = module.monitoring.workspace_id
  allowed_ip_ranges          = var.allowed_ip_ranges
  tags                       = local.tags
}

module "keyvault" {
  source                     = "./modules/keyvault"
  name                       = "kv-${local.name}"
  resource_group_name        = azurerm_resource_group.this.name
  location                   = var.location
  log_analytics_workspace_id = module.monitoring.workspace_id
  tags                       = local.tags
}

module "databricks" {
  source                     = "./modules/databricks_workspace"
  name                       = local.name
  resource_group_name        = azurerm_resource_group.this.name
  location                   = var.location
  storage_account_id         = module.storage.id
  log_analytics_workspace_id = module.monitoring.workspace_id
  tags                       = local.tags
}

module "data_factory" {
  source                     = "./modules/data_factory"
  name                       = "adf-${local.name}"
  resource_group_name        = azurerm_resource_group.this.name
  location                   = var.location
  storage_account_id         = module.storage.id
  storage_dfs_endpoint       = module.storage.dfs_endpoint
  key_vault_id               = module.keyvault.id
  log_analytics_workspace_id = module.monitoring.workspace_id
  tags                       = local.tags
}

module "unity_catalog" {
  source = "./modules/unity_catalog"
  providers = {
    databricks = databricks.workspace
  }
  env                  = var.env
  metastore_id         = var.metastore_id
  assign_metastore     = var.assign_metastore
  workspace_numeric_id = module.databricks.workspace_numeric_id
  access_connector_id  = module.databricks.access_connector_id
  storage_account_name = module.storage.name
  key_vault_id         = module.keyvault.id
  key_vault_uri        = module.keyvault.uri
  data_engineers_group = var.data_engineers_group
  analysts_group       = var.analysts_group
}
