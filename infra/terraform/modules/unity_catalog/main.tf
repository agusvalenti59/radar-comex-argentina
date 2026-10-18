terraform {
  required_providers {
    databricks = { source = "databricks/databricks" }
  }
}

variable "env" { type = string }
variable "metastore_id" { type = string }
variable "workspace_numeric_id" { type = string }
variable "access_connector_id" { type = string }
variable "storage_account_name" { type = string }
variable "key_vault_id" { type = string }
variable "key_vault_uri" { type = string }
variable "data_engineers_group" { type = string }
variable "analysts_group" { type = string }

locals {
  catalog = "trade_${var.env}"
  abfss   = { for c in ["landing", "lakehouse", "checkpoints"] : c => "abfss://${c}@${var.storage_account_name}.dfs.core.windows.net/" }
  schemas = {
    bronze = "Datos crudos tal cual llegaron de cada fuente, con metadata técnica"
    silver = "Flujos comerciales unificados, limpios y conformados (HS6, ISO3)"
    gold   = "Modelo dimensional y métricas de inteligencia comercial"
    ops    = "Auditoría de corridas, métricas de calidad, cuarentena y frescura"
  }
}

resource "databricks_metastore_assignment" "this" {
  metastore_id = var.metastore_id
  workspace_id = var.workspace_numeric_id
}

resource "databricks_storage_credential" "lake" {
  name = "sc-trade-${var.env}"
  azure_managed_identity { access_connector_id = var.access_connector_id }
  comment    = "Managed identity del Access Connector"
  depends_on = [databricks_metastore_assignment.this]
}

resource "databricks_external_location" "this" {
  for_each        = local.abfss
  name            = "el-trade-${var.env}-${each.key}"
  url             = each.value
  credential_name = databricks_storage_credential.lake.name
}

resource "databricks_catalog" "this" {
  name           = local.catalog
  storage_root   = "${local.abfss["lakehouse"]}${local.catalog}"
  comment        = "Trade Intelligence (${var.env})"
  isolation_mode = "ISOLATED"
  properties     = { domain = "trade-intelligence" }
  depends_on     = [databricks_external_location.this]
}

resource "databricks_schema" "this" {
  for_each     = local.schemas
  catalog_name = databricks_catalog.this.name
  name         = each.key
  comment      = each.value
}

# Volume para seeds de referencia (countries.csv, products.csv) que sube el CD
resource "databricks_volume" "reference" {
  catalog_name = databricks_catalog.this.name
  schema_name  = databricks_schema.this["ops"].name
  name         = "reference"
  volume_type  = "MANAGED"
}

# Secret scope respaldado por Key Vault: los jobs leen secretos sin que pasen por Databricks
resource "databricks_secret_scope" "kv" {
  name = "kv-trade-intel"
  keyvault_metadata {
    resource_id = var.key_vault_id
    dns_name    = var.key_vault_uri
  }
}

# --- Permisos (principio de mínimo privilegio) ---
resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.this.name
  grant {
    principal  = var.data_engineers_group
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE", "CREATE_VOLUME", "READ_VOLUME", "WRITE_VOLUME"]
  }
  grant {
    principal  = var.analysts_group
    privileges = ["USE_CATALOG"]
  }
}

resource "databricks_grants" "gold" {
  schema = "${databricks_catalog.this.name}.${databricks_schema.this["gold"].name}"
  grant {
    principal  = var.analysts_group
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

resource "databricks_grants" "landing" {
  external_location = databricks_external_location.this["landing"].id
  grant {
    principal  = var.data_engineers_group
    privileges = ["READ_FILES", "WRITE_FILES"]
  }
}

# SQL Warehouse serverless para Power BI / Databricks SQL
resource "databricks_sql_endpoint" "bi" {
  name                      = "wh-trade-bi-${var.env}"
  cluster_size              = var.env == "prod" ? "Small" : "2X-Small"
  max_num_clusters          = var.env == "prod" ? 3 : 1
  auto_stop_mins            = 10
  enable_serverless_compute = true
  warehouse_type            = "PRO"
  tags {
    custom_tags {
      key   = "cost_center"
      value = "trade-intel"
    }
  }
}

resource "databricks_permissions" "warehouse" {
  sql_endpoint_id = databricks_sql_endpoint.bi.id
  access_control {
    group_name       = var.analysts_group
    permission_level = "CAN_USE"
  }
}

output "catalog_name" { value = databricks_catalog.this.name }
output "warehouse_id" { value = databricks_sql_endpoint.bi.id }
