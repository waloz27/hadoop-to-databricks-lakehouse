variable "location" {
  description = "Región de Azure (la única con máquinas utilizables en esta suscripción)"
  type        = string
  default     = "brazilsouth"
}

variable "resource_group_name" {
  type    = string
  default = "rg-lakehouse-lab"
}

variable "workspace_name" {
  type    = string
  default = "dbw-lakehouse-lab"
}

variable "tags" {
  type = map(string)
  default = {
    proyecto = "hadoop-to-databricks-lakehouse"
    entorno  = "lab"
  }
}