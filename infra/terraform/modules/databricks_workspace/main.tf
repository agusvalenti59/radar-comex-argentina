variable "name" { type = string }
variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "storage_account_id" { type = string }
variable "log_analytics_workspace_id" { type = string }
variable "tags" { type = map(string) }

resource "azurerm_databricks_workspace" "this" {
  name                        = "dbw-${var.name}"
  resource_group_name         = var.resource_group_name
  location                    = var.location
  sku                         = "premium" # requerido para Unity Catalog
  managed_resource_group_name = "rg-${var.name}-dbw-managed"
  tags                        = var.tags
}

# Identidad administrada que Unity Catalog usa para leer/escribir ADLS (sin secretos)
resource "azurerm_databricks_access_connector" "this" {
  name                = "dac-${var.name}"
  resource_group_name = var.resource_group_name
  location            = var.location
  identity { type = "SystemAssigned" }
  tags = var.tags
}

resource "azurerm_role_assignment" "uc_storage" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
}

resource "azurerm_monitor_diagnostic_setting" "dbw" {
  name                       = "diag-dbw"
  target_resource_id         = azurerm_databricks_workspace.this.id
  log_analytics_workspace_id = var.log_analytics_workspace_id
  enabled_log { category = "jobs" }
  enabled_log { category = "clusters" }
  enabled_log { category = "unityCatalog" }
  enabled_log { category = "accounts" }
}

output "workspace_url" { value = "https://${azurerm_databricks_workspace.this.workspace_url}" }
output "workspace_id" { value = azurerm_databricks_workspace.this.id }
output "workspace_numeric_id" { value = azurerm_databricks_workspace.this.workspace_id }
output "access_connector_id" { value = azurerm_databricks_access_connector.this.id }
