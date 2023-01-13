<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging\Workspace;

use Keboola\StagingProvider\Staging\StagingInterface;

interface WorkspaceStagingInterface extends StagingInterface
{
    public function __construct(array $data);

    public function getWorkspaceId(): string;

    public function getCredentials(): array;

    public function getBackendSize(): ?string;
}
