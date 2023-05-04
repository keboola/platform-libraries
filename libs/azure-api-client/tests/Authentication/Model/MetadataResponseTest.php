<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Authentication\Model;

use Keboola\AzureApiClient\Authentication\Model\MetadataResponse;
use PHPUnit\Framework\TestCase;

class MetadataResponseTest extends TestCase
{
    public function testFromResponseData(): void
    {
        // partial response from https://management.azure.com/metadata/endpoints?api-version=2020-01-01
        $data = [
            'portal' => 'https://portal.azure.com',
            'authentication' => [
                'loginEndpoint' => 'https://login.microsoftonline.com/',
                'audiences' => [
                    'https://management.core.windows.net/',
                    'https://management.azure.com/',
                ],
                'tenant' => 'common',
                'identityProvider' => 'AAD',
            ],
            'media' => 'https://rest.media.azure.net',
            'graphAudience' => 'https://graph.windows.net/',
            'graph' => 'https://graph.windows.net/',
            'name' => 'AzureCloud',
            'suffixes' => [
                'azureDataLakeStoreFileSystem' => 'azuredatalakestore.net',
                'acrLoginServer' => 'azurecr.io',
                'sqlServerHostname' => 'database.windows.net',
                'azureDataLakeAnalyticsCatalogAndJob' => 'azuredatalakeanalytics.net',
                'keyVaultDns' => 'vault.azure.net',
                'storage' => 'core.windows.net',
                'azureFrontDoorEndpointSuffix' => 'azurefd.net',
            ],
            'batch' => 'https://batch.core.windows.net/',
            'resourceManager' => 'https://management.azure.com/',
            // phpcs:ignore Generic.Files.LineLength
            'vmImageAliasDoc' => 'https://raw.githubusercontent.com/Azure/azure-rest-api-specs/master/arm-compute/quickstart-templates/aliases.json',
            'activeDirectoryDataLake' => 'https://datalake.azure.net/',
            'sqlManagement' => 'https://management.core.windows.net:8443/',
            'gallery' => 'https://gallery.azure.com/',
        ];

        $response = MetadataResponse::fromResponseData($data);

        self::assertSame('AzureCloud', $response->name);
        self::assertSame('vault.azure.net', $response->keyVaultDnsSuffix);
        self::assertSame('https://login.microsoftonline.com/', $response->authenticationLoginEndpoint);
    }
}
