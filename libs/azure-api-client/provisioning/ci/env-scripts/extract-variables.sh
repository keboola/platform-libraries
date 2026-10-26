#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source ./functions.sh

# output variables
output_var 'AZURE_API_CLIENT_TEST__TENANT_ID' "$(terraform_output 'az_tenant_id')"
output_var 'AZURE_API_CLIENT_TEST__CLIENT_ID' "$(terraform_output 'az_application_id')"
output_var 'AZURE_API_CLIENT_TEST__CLIENT_SECRET' "$(terraform_output 'az_application_secret')"
echo ''

output_var 'AZURE_API_CLIENT_TEST__SERVICE_BUS_ENDPOINT' "$(terraform_output 'az_servicebus_endpoint')"
output_var 'AZURE_API_CLIENT_TEST__SERVICE_BUS_SAS_KEY_NAME' "$(terraform_output 'az_servicebus_sas_key_name')"
output_var 'AZURE_API_CLIENT_TEST__SERVICE_BUS_SAS_KEY' "$(terraform_output 'az_servicebus_sas_key_value')"
output_var 'AZURE_API_CLIENT_TEST__SERVICE_BUS_QUEUE' "$(terraform_output 'az_servicebus_queue_name')"
echo ''

output_var 'AZURE_API_CLIENT_TEST__EVENT_GRID_TOPIC_HOSTNAME' "$(terraform_output 'az_eventgrid_topic_hostname')"
output_var 'AZURE_API_CLIENT_TEST__EVENT_GRID_TOPIC_NAME' "$(terraform_output 'az_eventgrid_topic_name')"
output_var 'AZURE_API_CLIENT_TEST__EVENT_GRID_ACCESS_KEY' "$(terraform_output 'az_eventgrid_access_key')"
output_var 'AZURE_API_CLIENT_TEST__EVENT_GRID_QUEUE' "$(terraform_output 'az_eventgrid_queue_name')"
echo ''
