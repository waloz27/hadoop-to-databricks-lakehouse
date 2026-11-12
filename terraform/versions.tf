terraform {
  required_version = ">= 1.5"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}
  # Los proveedores de recursos ya están registrados; evita que Terraform intente registrar otros.
  resource_provider_registrations = "none"
}