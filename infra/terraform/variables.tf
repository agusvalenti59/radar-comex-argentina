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
  description = "Metastore de Unity Catalog de la región (uno por región, se crea una sola vez a nivel cuenta)"
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
