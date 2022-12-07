<?php

namespace Keboola\StagingProvider\Staging\Workspace;

class BigQueryWorkspaceStaging extends WorkspaceStaging
{
    public static function getType()
    {
        return 'bigquery';
    }

    public function getCredentials()
    {
        $connection = $this->data['connection'];

        return [
            'schema' => $connection['schema'],
            'credentials' => $connection['credentials'],
        ];
    }
}
