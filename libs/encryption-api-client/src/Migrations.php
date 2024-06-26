<?php

declare(strict_types=1);

namespace Keboola\EncryptionApiClient;

class Migrations extends Common
{
    public function __construct(string $manageApiToken, array $config)
    {
        parent::__construct(['X-KBC-ManageApiToken' => $manageApiToken], $config);
    }

    public function migrateConfiguration(
        string $sourceStorageApiToken,
        string $destinationStack,
        string $destinationStorageApiToken,
        string $componentId,
        string $configId,
        string $branchId,
        bool $dryRun = false
    ): array {
        $queryParams = [];
        if ($dryRun) {
            $queryParams['dry-run'] = 'true';
        }

        $url = 'migrate-configuration';
        if (count($queryParams) > 0) {
            $url .= '?' . http_build_query($queryParams);
        }

        return $this->apiPost($url, [
            'sourceStorageApiToken' => $sourceStorageApiToken,
            'destinationStack' => $destinationStack,
            'destinationStorageApiToken' => $destinationStorageApiToken,
            'componentId' => $componentId,
            'configId' => $configId,
            'branchId' => $branchId,
        ]);
    }
}
