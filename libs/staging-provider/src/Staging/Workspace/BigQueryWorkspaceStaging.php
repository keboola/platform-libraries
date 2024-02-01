<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging\Workspace;

class BigQueryWorkspaceStaging extends WorkspaceStaging
{
    public static function getType(): string
    {
        return 'bigquery';
    }

    public function getCredentials(): array
    {
        $connection = $this->data['connection'];

        return [
            'schema' => $connection['schema'],
            'region' => $connection['region'],
            'credentials' => $connection['credentials'],
        ];
    }
}
