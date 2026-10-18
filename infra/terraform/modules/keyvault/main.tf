variable "name" { type = string }
variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "log_analytics_workspace_id" { type = string }
variable "tags" { type = map(string) }

data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "this" {
  name                       = substr(replace(var.name, "_", "-"), 0, 24)
  resource_group_name        = var.resource_group_name
  location                   = var.location
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  enable_rbac_authorization  = true
  purge_protection_enabled   = true
  soft_delete_retention_days = 30
  tags                       = var.tags
}

# El valor real lo carga un humano / pipeline de secretos; Terraform sólo crea el placeholder.
resource "azurerm_key_vault_secret" "comtrade_key" {
  name         = "comtrade-subscription-key"
  value        = "CHANGE_ME"
  key_vault_id = azurerm_key_vault.this.id
  lifecycle { ignore_changes = [value] }
}

resource "azurerm_monitor_diagnostic_setting" "kv" {
  name                       = "diag-kv"
  target_resource_id         = azurerm_key_vault.this.id
  log_analytics_workspace_id = var.log_analytics_workspace_id
  enabled_log { category = "AuditEvent" }
}

output "id" { value = azurerm_key_vault.this.id }
output "uri" { value = azurerm_key_vault.this.vault_uri }
