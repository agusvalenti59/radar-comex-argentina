variable "name" { type = string }
variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "containers" { type = list(string) }
variable "replication" { type = string }
variable "log_analytics_workspace_id" { type = string }
variable "allowed_ip_ranges" { type = list(string) }
variable "tags" { type = map(string) }

resource "azurerm_storage_account" "this" {
  name                            = var.name
  resource_group_name             = var.resource_group_name
  location                        = var.location
  account_tier                    = "Standard"
  account_replication_type        = var.replication
  account_kind                    = "StorageV2"
  is_hns_enabled                  = true # ADLS Gen2
  min_tls_version                 = "TLS1_2"
  shared_access_key_enabled       = false # sólo Entra ID / managed identities
  allow_nested_items_to_be_public = false
  default_to_oauth_authentication = true

  blob_properties {
    delete_retention_policy { days = 14 }
    container_delete_retention_policy { days = 14 }
  }

  network_rules {
    default_action = length(var.allowed_ip_ranges) > 0 ? "Deny" : "Allow"
    ip_rules       = var.allowed_ip_ranges
    bypass         = ["AzureServices"]
  }
  tags = var.tags
}

resource "azurerm_storage_container" "this" {
  for_each              = toset(var.containers)
  name                  = each.value
  storage_account_id    = azurerm_storage_account.this.id
  container_access_type = "private"
}

# Landing: archivos crudos pasan a cool a los 30 días y se borran al año (ya están en bronze)
resource "azurerm_storage_management_policy" "landing" {
  storage_account_id = azurerm_storage_account.this.id
  rule {
    name    = "landing-lifecycle"
    enabled = true
    filters {
      prefix_match = ["landing/"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than = 30
        delete_after_days_since_modification_greater_than       = 365
      }
    }
  }
}

resource "azurerm_monitor_diagnostic_setting" "blob" {
  name                       = "diag-blob"
  target_resource_id         = "${azurerm_storage_account.this.id}/blobServices/default"
  log_analytics_workspace_id = var.log_analytics_workspace_id
  enabled_log { category = "StorageWrite" }
  enabled_log { category = "StorageDelete" }
  enabled_metric { category = "Transaction" }
}

output "id" { value = azurerm_storage_account.this.id }
output "name" { value = azurerm_storage_account.this.name }
output "dfs_endpoint" { value = azurerm_storage_account.this.primary_dfs_endpoint }
