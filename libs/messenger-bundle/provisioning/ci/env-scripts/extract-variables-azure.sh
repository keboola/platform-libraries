#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source ./functions.sh

# output variables
output_var 'TEST_CLOUD_PLATFORM' 'azure'
output_var 'CONNECTION_EVENTS_QUEUE_DSN' "$(print_url 'azure://%s:%s@%s?entity_path=%s' "$(terraform_output 'az_servicebus_sas_key_name')" "$(terraform_output 'az_servicebus_sas_key_value')" "$(terraform_output 'az_servicebus_namespace')" "$(terraform_output 'az_servicebus_queue_name')")"
output_var 'CONNECTION_AUDIT_LOG_QUEUE_DSN' "$(print_url 'azure://%s:%s@%s?entity_path=%s' "$(terraform_output 'az_servicebus_sas_key_name')" "$(terraform_output 'az_servicebus_sas_key_value')" "$(terraform_output 'az_servicebus_namespace')" "$(terraform_output 'az_servicebus_queue_name')")"
echo ''

output_var 'AZURE_TENANT_ID' "$(terraform_output 'az_tenant_id')"
output_var 'AZURE_CLIENT_ID' "$(terraform_output 'az_application_id')"
output_var 'AZURE_CLIENT_SECRET' "$(terraform_output 'az_application_secret')"
echo ''
