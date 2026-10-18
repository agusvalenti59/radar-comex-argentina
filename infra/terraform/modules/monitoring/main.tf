variable "name" { type = string }
variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "tags" { type = map(string) }

resource "azurerm_log_analytics_workspace" "this" {
  name                = "log-${var.name}"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = var.tags
}

resource "azurerm_monitor_action_group" "data_alerts" {
  name                = "ag-${var.name}-data"
  resource_group_name = var.resource_group_name
  short_name          = "dataalerts"
  email_receiver {
    name          = "data-team"
    email_address = "data-alerts@example.com"
  }
  tags = var.tags
}

# Alerta: corridas fallidas de ADF
resource "azurerm_monitor_scheduled_query_rules_alert_v2" "adf_failures" {
  name                 = "alert-${var.name}-adf-failures"
  resource_group_name  = var.resource_group_name
  location             = var.location
  scopes               = [azurerm_log_analytics_workspace.this.id]
  severity             = 2
  evaluation_frequency = "PT15M"
  window_duration      = "PT15M"
  criteria {
    query                   = "ADFPipelineRun | where Status == 'Failed'"
    time_aggregation_method = "Count"
    operator                = "GreaterThan"
    threshold               = 0
  }
  action { action_groups = [azurerm_monitor_action_group.data_alerts.id] }
  tags = var.tags
}

output "workspace_id" { value = azurerm_log_analytics_workspace.this.id }
output "action_group_id" { value = azurerm_monitor_action_group.data_alerts.id }
