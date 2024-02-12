data "azurerm_client_config" "current" {}

data "azurerm_resource_group" "existing" {
  name = "wizeline-capstone"
}

locals {
  current_user_id = coalesce(var.msi_id, data.azurerm_client_config.current.object_id)
}

resource "azurerm_key_vault" "wizeline_vault" {
  name                       = var.vault_name
  location                   = data.azurerm_resource_group.existing.location
  resource_group_name        = data.azurerm_resource_group.existing.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = var.sku_name
  soft_delete_retention_days = 7

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = local.current_user_id

    key_permissions    = var.key_permissions
    secret_permissions = var.secret_permissions
  } 
}

resource "azurerm_key_vault_access_policy" "secret_policy" {
  key_vault_id = azurerm_key_vault.wizeline_vault.id

  tenant_id = data.azurerm_client_config.current.tenant_id
  object_id = var.portal_user_id

  secret_permissions = ["Get", "List"]
}


resource "azurerm_key_vault_secret" "secret" {
  count          = length(var.secrets)
  name           = var.secrets[count.index].name
  value          = var.secrets[count.index].value
  key_vault_id   = azurerm_key_vault.wizeline_vault.id
}

