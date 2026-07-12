#!/usr/bin/env bash
# ============================================================
# Databricks Secret Scope Setup
# Run once from your local machine (needs Databricks CLI installed)
#
# Install CLI:  pip install databricks-cli
# Configure:    databricks configure --token
#               Host: https://<workspace>.azuredatabricks.net
#               Token: <your-PAT>
# ============================================================

SCOPE="hpe-forecast"

# Create secret scope (Databricks-managed, no Key Vault needed)
databricks secrets create-scope --scope $SCOPE

# ── ADLS Gen2 ──────────────────────────────────────────────
databricks secrets put --scope $SCOPE --key adls-account-name
# When prompted, enter: hpeforecastadls

databricks secrets put --scope $SCOPE --key adls-account-key
# When prompted, enter: <storage account key1 from Azure Portal>

# ── SAP HANA Cloud ─────────────────────────────────────────
databricks secrets put --scope $SCOPE --key saphana-host
# When prompted, enter: <your-instance>.hanacloud.ondemand.com

databricks secrets put --scope $SCOPE --key saphana-user
# When prompted, enter: DBADMIN

databricks secrets put --scope $SCOPE --key saphana-password
# When prompted, enter: <your BTP Trial HANA password>

# ── SQL Server ─────────────────────────────────────────────
databricks secrets put --scope $SCOPE --key sqlserver-host
databricks secrets put --scope $SCOPE --key sqlserver-db
databricks secrets put --scope $SCOPE --key sqlserver-user
databricks secrets put --scope $SCOPE --key sqlserver-password

# ── Salesforce ─────────────────────────────────────────────
databricks secrets put --scope $SCOPE --key salesforce-username
# When prompted, enter: vinayaksdesai777@gmail.com

databricks secrets put --scope $SCOPE --key salesforce-password
databricks secrets put --scope $SCOPE --key salesforce-security-token

# ── Service Principal (for ADLS OAuth mount in Databricks) ─
databricks secrets put --scope $SCOPE --key sp-client-id
databricks secrets put --scope $SCOPE --key sp-client-secret
databricks secrets put --scope $SCOPE --key sp-tenant-id

# ── Verify ─────────────────────────────────────────────────
echo ""
echo "Secrets in scope '$SCOPE':"
databricks secrets list --scope $SCOPE
