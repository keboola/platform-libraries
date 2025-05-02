<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\InputMapping\Staging\ProviderInterface;

interface WorkspaceProviderInterface extends ProviderInterface
{
    public function getBackendSize(): ?string;

    public function getBackendType(): string;

    public function resetCredentials(array $params): void;
}
