<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Throwable;

readonly class StorageApiWorkspace
{
    public function __construct(
        public string $id,
        public string $backend,
        public ?string $backendSize,
        /** @var array{
         *     host?: string,
         *     warehouse?: string,
         *     database?: string,
         *     schema?: string,
         *     user?: string,
         *     password?: string,
         *     account?: string,
         *     container?: string,
         *     connectionString?: string} $connection
         * */
        public array $credentials,
    ) {
    }

    public static function fromDataArray(array $data): self
    {
        try {
            $connection = match ($data['connection']['backend']) {
                'bigquery' => [
                    'schema' => $data['connection']['schema'],
                    'region' => $data['connection']['region'],
                    'credentials' => $data['connection']['credentials'],
                ],
                'exasol', 'redshift', 'synapse', 'teradata' => [
                    'host' => $data['connection']['host'],
                    'warehouse' => $data['connection']['warehouse'],
                    'database' => $data['connection']['database'],
                    'schema' => $data['connection']['schema'],
                    'user' => $data['connection']['user'],
                    'password' => $data['connection']['password'],
                ],
                'snowflake' => [
                    'host' => $data['connection']['host'],
                    'warehouse' => $data['connection']['warehouse'],
                    'database' => $data['connection']['database'],
                    'schema' => $data['connection']['schema'],
                    'user' => $data['connection']['user'],
                    'password' => $data['connection']['password'],
                    'account' => self::parseSnowflakeAccount($data['connection']['host']),
                ],
                'abs' => [
                    'container' => $data['connection']['container'],
                    'connectionString' => $data['connection']['connectionString'],
                ],
                default => throw new StagingProviderException(
                    sprintf('Unsupported backend "%s"', $data['connection']['backend']),
                ),
            };
        } catch (Throwable $e) {
            throw new StagingProviderException(
                'Invalid workspace connection data: ' . $e->getMessage(),
                0,
                $e,
            );
        }
        return new self(
            (string) $data['id'],
            $data['connection']['backend'],
            $data['backendSize'],
            $connection,
        );
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
