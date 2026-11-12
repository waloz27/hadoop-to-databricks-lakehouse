resource "azurerm_resource_group" "lab" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_databricks_workspace" "lab" {
  name                        = var.workspace_name
  resource_group_name         = azurerm_resource_group.lab.name
  location                    = azurerm_resource_group.lab.location
  sku                         = "premium"
  managed_resource_group_name = "databricks-rg-${var.workspace_name}"
  tags                        = var.tags
}