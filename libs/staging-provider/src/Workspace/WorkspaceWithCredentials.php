<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StorageApi\WorkspaceLoginType;
use SensitiveParameter;
use Throwable;

/**
 * @phpstan-type CredentialsArray array{
 *       container?: string|null,
 *       connectionString?: string|null,
 *       host?: string|null,
 *       warehouse?: string|null,
 *       database?: string|null,
 *       schema?: string|null,
 *       user?: string|null,
 *       password?: string|null,
 *       privateKey?: string|null,
 *       account?: string|null,
 *       credentials?: array|null,
 *  }
 */
class WorkspaceWithCredentials implements WorkspaceWithCredentialsInterface
{
    public function __construct(
        private readonly WorkspaceInterface $workspace,
        #[SensitiveParameter] private readonly array $credentials,
    ) {
    }

    public static function createFromData(
        #[SensitiveParameter] array $workspaceData,
    ): self {
        try {
            return new self(
                workspace: Workspace::createFromData($workspaceData),
                credentials: self::parseCredentialsData($workspaceData['connection']),
            );
        } catch (Throwable $e) {
            throw new StagingProviderException(
                'Invalid workspace data: ' . $e->getMessage(),
                previous: $e,
            );
        }
    }

    public function getWorkspaceId(): string
    {
        return $this->workspace->getWorkspaceId();
    }

    public function getBackendType(): string
    {
        return $this->workspace->getBackendType();
    }

    public function getBackendSize(): ?string
    {
        return $this->workspace->getBackendSize();
    }

    public function getLoginType(): WorkspaceLoginType
    {
        return $this->workspace->getLoginType();
    }

    /**
     * @return CredentialsArray
     */
    public function getCredentials(): array
    {
        return $this->credentials;
    }

    public static function parseCredentialsData(
        #[SensitiveParameter] array $data,
    ): array {
        $backend = $data['backend'];

        return match ($backend) {
            'bigquery' => [
                'schema' => $data['schema'],
                'region' => $data['region'],
                'credentials' => $data['credentials'],
            ],
            'snowflake' => [
                'host' => $data['host'],
                'warehouse' => $data['warehouse'],
                'database' => $data['database'],
                'schema' => $data['schema'],
                'user' => $data['user'],
                'password' => $data['password'] ?? null,
                'privateKey' => $data['privateKey'] ?? null,
                'account' => self::parseSnowflakeAccount($data['host']),
            ],
            default => throw new StagingProviderException(sprintf('Unsupported backend "%s"', $backend)),
        };
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
