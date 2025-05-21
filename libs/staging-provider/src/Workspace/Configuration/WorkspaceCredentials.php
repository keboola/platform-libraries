<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace\Configuration;

use Keboola\StagingProvider\Exception\StagingProviderException;
use SensitiveParameter;
use Throwable;

class WorkspaceCredentials
{
    public function __construct(
        #[SensitiveParameter] private readonly array $credentials,
    ) {
    }

    public static function fromData(
        #[SensitiveParameter] array $data,
    ): self {
        try {
            $backend = $data['backend'];

            $credentials = match ($backend) {
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
        } catch (Throwable $e) {
            throw new StagingProviderException(
                'Invalid credentials data: ' . $e->getMessage(),
                previous: $e,
            );
        }

        return new self($credentials);
    }

    public function toArray(): array
    {
        return $this->credentials;
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
