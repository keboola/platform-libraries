<?php

namespace Keboola\WorkspaceProvider\Staging\Workspace;

class AbsWorkspaceStaging extends WorkspaceStaging
{
    public static function getType()
    {
        return 'abs';
    }

    public function getCredentials()
    {
        $connection = $this->data['connection'];

        return [
            'connectionString' => $connection['connectionString'],
            'container' => $connection['container'],
        ];
    }
}
