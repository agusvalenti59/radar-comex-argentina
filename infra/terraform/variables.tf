variable "subscription_id" { type = string }
variable "env" {
  type = string
  validation {
    condition     = contains(["dev", "prod"], var.env)
    error_message = "env debe ser dev o prod."
  }
}
variable "location" {
  type    = string
  default = "brazilsouth" # región más cercana a AR con Databricks + ADF
}
variable "project" {
  type    = string
  default = "tradeintel"
}
variable "metastore_id" {
  type        = string
  default     = ""
  description = "Metastore de Unity Catalog de la región. Sólo hace falta si assign_metastore = true"
}
variable "assign_metastore" {
  type        = bool
  default     = false
  description = "true si el workspace NO quedó con Unity Catalog habilitado automáticamente"
}
variable "data_engineers_group" {
  type    = string
  default = "data-engineers"
}
variable "analysts_group" {
  type    = string
  default = "trade-analysts"
}
variable "allowed_ip_ranges" {
  type    = list(string)
  default = []
}
variable "tags" {
  type    = map(string)
  default = {}
}
