variable "name" { type = string }
variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "storage_account_id" { type = string }
variable "storage_dfs_endpoint" { type = string }
variable "key_vault_id" { type = string }
variable "log_analytics_workspace_id" { type = string }
variable "tags" { type = map(string) }

resource "azurerm_data_factory" "this" {
  name                = var.name
  resource_group_name = var.resource_group_name
  location            = var.location
  identity { type = "SystemAssigned" }
  # En dev se conecta a GitHub para editar en el portal; prod se publica sólo desde CI.
  tags = var.tags
}

resource "azurerm_role_assignment" "adf_storage" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.this.identity[0].principal_id
}

resource "azurerm_role_assignment" "adf_kv" {
  scope                = var.key_vault_id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_data_factory.this.identity[0].principal_id
}

resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "adls" {
  name                 = "ls_adls_lake"
  data_factory_id      = azurerm_data_factory.this.id
  url                  = var.storage_dfs_endpoint
  use_managed_identity = true
}

resource "azurerm_data_factory_linked_service_web" "indec" {
  name                = "ls_http_indec"
  data_factory_id     = azurerm_data_factory.this.id
  authentication_type = "Anonymous"
  url                 = "https://comex.indec.gob.ar"
}

resource "azurerm_monitor_diagnostic_setting" "adf" {
  name                           = "diag-adf"
  target_resource_id             = azurerm_data_factory.this.id
  log_analytics_workspace_id     = var.log_analytics_workspace_id
  log_analytics_destination_type = "Dedicated"
  enabled_log { category = "PipelineRuns" }
  enabled_log { category = "ActivityRuns" }
  enabled_log { category = "TriggerRuns" }
}

output "name" { value = azurerm_data_factory.this.name }
output "principal_id" { value = azurerm_data_factory.this.identity[0].principal_id }
