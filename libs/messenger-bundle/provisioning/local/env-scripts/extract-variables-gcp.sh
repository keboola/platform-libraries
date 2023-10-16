#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source ./functions.sh

# output variables
output_var 'TEST_CLOUD_PLATFORM' 'gcp'
output_var 'CONNECTION_EVENTS_QUEUE_DSN' "$(print_url 'gps://default/%s' "$(terraform_output 'gcp_topic_name')")"
echo ''

output_file 'var/gcp/private-key.json' "$(terraform_output 'gcp_application_credentials' | base64 --decode)"
output_var 'GOOGLE_APPLICATION_CREDENTIALS' 'var/gcp/private-key.json'
echo ''
