<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging\Workspace;

class SnowflakeWorkspaceStaging extends WorkspaceStaging
{
    public static function getType(): string
    {
        return 'snowflake';
    }

    public function getCredentials(): array
    {
        $credentials = parent::getCredentials();
        $credentials['account'] = $this->parseAccount($credentials['host']);

        return $credentials;
    }

    /**
     * Parses `account` from `host`
     *
     * Based on how Snowflake Python Connector handles account resolution.
     * https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/connector/util_text.py#L242
     */
    private function parseAccount(string $host): string
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
