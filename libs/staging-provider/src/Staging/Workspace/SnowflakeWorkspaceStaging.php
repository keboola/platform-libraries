<?php

namespace Keboola\StagingProvider\Staging\Workspace;

class SnowflakeWorkspaceStaging extends WorkspaceStaging
{
    public static function getType()
    {
        return 'snowflake';
    }

    public function getCredentials()
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

        return substr($hostParts[0], 0, strrpos($hostParts[0], '-'));
    }
}
