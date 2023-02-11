# Plarform Libraries

## License

MIT licensed, see [LICENSE](./LICENSE) file.

## CI setup

### Auto Stop for Azure Synapse Analytics

The instructions reflect the current setup when the server is already created and belongs to the `Keboola DEV Connection Team` subscription.

Prerequisites:
* locally installed `terraform`
    * https://www.terraform.io
* configured `az` CLI tools (run `az login`)
* existing Azure Synapse Analytics

#### Prepare resoruces

```shell
cat <<EOF > ./provisioning/ci/synapse-auto-stop/terraform.tfvars
resource_group = "{SYNAPSE_AZURE_RESOURCE_GROUP}"
synapse_server_name = "{SYNAPSE_SERVER_NAME}"
EOF

terraform -chdir=./provisioning/ci/synapse-auto-stop init
terraform -chdir=./provisioning/ci/synapse-auto-stop apply

./provisioning/local/update-env.sh azure # or aws
```

It creates new resources in resource group of Synapse server and new application in Active Directory and will output following infromations:
- `application_account` - Application name registered in Azure Active Directory
- `runbook` - Name of the runbook with Synapse pause prodcedure
- `schedule` - Name of the schedule linked to your runbook

#### Finalize configuration

Go to the Azure Portal and then:

- In [Subscription IAM configuration](https://portal.azure.com/#@keboolaconnection.onmicrosoft.com/resource/subscriptions/eac4eb61-1abe-47e2-a0a1-f0a7e066f385/users) add your application with `Contributor` role.
  _If you do not have permissions for this operation, ask SRE team for that._
- In resource group of Synapse server find your runbook and set starts time for your schedule. 
