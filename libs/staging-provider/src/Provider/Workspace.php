<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StorageApi\WorkspaceLoginType;
use Throwable;

/**
 * @internal
 */
class Workspace
{
    private ?array $credentials = null;

    private function __construct(
        private readonly string $id,
        private readonly string $backendType,
        private readonly ?string $backendSize,
        private readonly WorkspaceLoginType $loginType,
        private readonly array $connectionData,
    ) {
    }

    public static function createFromData(array $workspaceData): self
    {
        try {
            return new self(
                id: (string) $workspaceData['id'],
                backendType: $workspaceData['connection']['backend'],
                backendSize: $workspaceData['backendSize'] ?? null,
                loginType: WorkspaceLoginType::from(
                    $workspaceData['connection']['loginType'] ?? WorkspaceLoginType::DEFAULT->value,
                ),
                connectionData: $workspaceData['connection'],
            );
        } catch (Throwable $e) {
            throw new StagingProviderException(
                'Invalid workspace data: ' . $e->getMessage(),
                previous: $e,
            );
        }
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function getBackendType(): string
    {
        return $this->backendType;
    }

    public function getBackendSize(): ?string
    {
        return $this->backendSize;
    }

    public function getLoginType(): WorkspaceLoginType
    {
        return $this->loginType;
    }

    public function hasCredentials(): bool
    {
        return $this->credentials !== null;
    }

    public function getCredentials(): array
    {
        if ($this->credentials === null) {
            throw new StagingProviderException('Credentials are not available');
        }

        return $this->credentials;
    }

    public function setCredentialsFromData(?array $data): void
    {
        if ($data === null) {
            $this->credentials = null;
            return;
        }

        $connectionData = array_merge(
            $this->connectionData,
            $data,
        );

        try {
            $this->credentials = match ($this->backendType) {
                'bigquery' => [
                    'schema' => $connectionData['schema'],
                    'region' => $connectionData['region'],
                    'credentials' => $connectionData['credentials'],
                ],
                'exasol',
                'redshift',
                'synapse',
                'teradata' => [
                    'host' => $connectionData['host'],
                    'warehouse' => $connectionData['warehouse'],
                    'database' => $connectionData['database'],
                    'schema' => $connectionData['schema'],
                    'user' => $connectionData['user'],
                    'password' => $connectionData['password'],
                ],
                'snowflake' => [
                    'host' => $connectionData['host'],
                    'warehouse' => $connectionData['warehouse'],
                    'database' => $connectionData['database'],
                    'schema' => $connectionData['schema'],
                    'user' => $connectionData['user'],
                    'password' => $connectionData['password'] ?? null,
                    'privateKey' => $connectionData['privateKey'] ?? null,
                    'account' => self::parseSnowflakeAccount($connectionData['host']),
                ],
                'abs' => [
                    'container' => $connectionData['container'],
                    'connectionString' => $connectionData['connectionString'],
                ],
                default => throw new StagingProviderException(sprintf('Unsupported backend "%s"', $this->backendType)),
            };
        } catch (Throwable $e) {
            throw new StagingProviderException(
                'Invalid credentials data: ' . $e->getMessage(),
                previous: $e,
            );
        }
    }

    /**
     * Parses `account` from `host`
     *
     * Based on how Snowflake Python Connector handles account resolution.
     * https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/connector/util_text.py#L242
     */
    private static function parseSnowflakeAccount(string $host): string
    {
        $hostParts = explode('.', $host);

        if (count($hostParts) <= 1) {
            return $host;
        }

        if ($hostParts[1] !== 'global') {
            return $hostParts[0];
        }

        return substr($hostParts[0], 0, (int) strrpos($hostParts[0], '-'));
    }
}
