<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Sandboxes\Legacy;

use InvalidArgumentException;

class SandboxCredentials
{
    private string $type;
    private string $projectId;
    private string $privateKeyId;
    private string $clientEmail;
    private string $clientId;
    private string $authUri;
    private string $tokenUri;
    private string $authProviderCertUrl;
    private string $clientCertUrl;
    private string $privateKey;

    private static array $required = [
        'type',
        'project_id',
        'private_key_id',
        'client_email',
        'client_id',
        'auth_uri',
        'token_uri',
        'auth_provider_x509_cert_url',
        'client_x509_cert_url',
        'private_key',
    ];

    public static function fromArray(array $data): self
    {
        self::checkRequiredKeys($data);

        $instance = new self();
        $instance->type = $data['type'];
        $instance->projectId = $data['project_id'];
        $instance->privateKeyId = $data['private_key_id'];
        $instance->clientEmail = $data['client_email'];
        $instance->clientId = $data['client_id'];
        $instance->authUri = $data['auth_uri'];
        $instance->tokenUri = $data['token_uri'];
        $instance->authProviderCertUrl = $data['auth_provider_x509_cert_url'];
        $instance->clientCertUrl = $data['client_x509_cert_url'];
        $instance->privateKey = $data['private_key'];

        return $instance;
    }

    public function toArray(): array
    {
        return [
            'type' => $this->type,
            'project_id' => $this->projectId,
            'private_key_id' => $this->privateKeyId,
            'client_email' => $this->clientEmail,
            'client_id' => $this->clientId,
            'auth_uri' => $this->authUri,
            'token_uri' => $this->tokenUri,
            'auth_provider_x509_cert_url' => $this->authProviderCertUrl,
            'client_x509_cert_url' => $this->clientCertUrl,
            'private_key' => $this->privateKey,
        ];
    }

    private static function checkRequiredKeys(array $data): void
    {
        $diff = array_diff_key(array_combine(self::$required, self::$required), $data);
        if (!empty($diff)) {
            throw new InvalidArgumentException(
                sprintf('Missing credential field(s) "%s"', implode(',', $diff)),
            );
        }
    }
}
