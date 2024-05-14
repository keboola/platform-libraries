<?php

declare(strict_types=1);

namespace Keboola\EncryptionApiClient;

class Encryption extends Common
{
    public function __construct(string $sapiToken, array $config)
    {
        parent::__construct(['X-StorageApi-Token' => $sapiToken], $config);
    }

    public function encryptPlainTextForConfiguration(
        string $value,
        string $projectId,
        string $componentId,
        string $configId
    ): string {
        $url = 'encrypt?';
        $url .= http_build_query([
            'projectId' => $projectId,
            'componentId' => $componentId,
            'configId' => $configId,
        ]);

        // because at the moment Common client is made for json requests/responses,
        // we need to wrap the value in an array
        $result = $this->apiPost($url, ['#value' => $value]);
        return $result['#value'];
    }
}
