<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging\Workspace;

class AbsWorkspaceStaging extends WorkspaceStaging
{
    public static function getType(): string
    {
        return 'abs';
    }

    public function getCredentials(): array
    {
        $connection = $this->data['connection'];

        return [
            'connectionString' => $connection['connectionString'],
            'container' => $connection['container'],
        ];
    }
}
