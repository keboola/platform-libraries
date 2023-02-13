$connectionName = "${connection_name}"
try
{
    # Get the connection "AzureRunAsConnection "
    $servicePrincipalConnection=Get-AutomationConnection -Name $connectionName

    "Logging in to Azure..."
    Add-AzureRmAccount `
        -ServicePrincipal `
        -TenantId $servicePrincipalConnection.TenantId `
        -ApplicationId $servicePrincipalConnection.ApplicationId `
        -CertificateThumbprint $servicePrincipalConnection.CertificateThumbprint
}
catch {
    if (!$servicePrincipalConnection)
    {
        $ErrorMessage = "Connection $connectionName not found."
        throw $ErrorMessage
    } else{
        Write-Error -Message $_.Exception
        throw $_.Exception
    }
}

$group = "${resource_group}"
$server = "${synapse_server_name}"

$serverStatus = Get-AzureRmSqlDatabase `
    -ResourceGroupName $group `
    -ServerName $server `
    -DatabaseName $server

if($serverStatus.Status -eq "Online") {
    Write-Output "Pausing synapse server"

    Suspend-AzureRmSqlDatabase `
            -ResourceGroupName $group `
            -DatabaseName $server `
            -ServerName $server

} else {
    Write-Output ("Synapse is: " + $serverStatus.Status)
    Write-Output "Doing nothing"
}
