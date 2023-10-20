#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source ./functions.sh

# output variables
output_var 'TEST_CLOUD_PLATFORM' 'aws'
output_var 'CONNECTION_EVENTS_QUEUE_DSN' "$(terraform_output 'aws_sqs_queue_url')"
echo ''

output_var 'AWS_DEFAULT_REGION' "$(terraform_output 'aws_region')"
output_var 'AWS_ACCESS_KEY_ID' "$(terraform_output 'aws_access_key_id')"
output_var 'AWS_SECRET_ACCESS_KEY' "$(terraform_output 'aws_access_key_secret')"
echo ''
