<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace\Configuration;

use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StorageApi\WorkspaceLoginType;

readonly class NewWorkspaceConfig
{
    public function __construct(
        public StagingType $stagingType,
        public string $componentId,
        public ?string $configId,
        public ?string $size,
        public ?bool $useReadonlyRole,
        public NetworkPolicy $networkPolicy,
        public ?WorkspaceLoginType $loginType,
    ) {
    }
}
