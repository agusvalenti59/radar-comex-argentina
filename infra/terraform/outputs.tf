output "databricks_workspace_url" { value = module.databricks.workspace_url }
output "storage_account" { value = module.storage.name }
output "key_vault_uri" { value = module.keyvault.uri }
output "data_factory_name" { value = module.data_factory.name }
output "catalog" { value = module.unity_catalog.catalog_name }
