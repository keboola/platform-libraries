<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient\Model;

use Keboola\ApiClientBase\ResponseModelInterface;

final readonly class AppProduction implements ResponseModelInterface
{
    /**
     * @param non-empty-string|null $desiredConfigVersion
     * @param non-empty-string|null $actualConfigVersion
     */
    public function __construct(
        public ?string $desiredDeploymentId,
        public ?string $actualDeploymentId,
        public ?string $desiredConfigVersion,
        public ?string $actualConfigVersion,
        public ?string $desiredCreatedAt,
        public ?string $activeRunId = null,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        /**
         * @var array{
         *     desiredDeploymentId: string|null,
         *     actualDeploymentId: string|null,
         *     desiredConfigVersion: non-empty-string|null,
         *     actualConfigVersion: non-empty-string|null,
         *     desiredCreatedAt: string|null,
         *     activeRunId?: string|null,
         * } $data
         */
        return new self(
            $data['desiredDeploymentId'],
            $data['actualDeploymentId'],
            $data['desiredConfigVersion'],
            $data['actualConfigVersion'],
            $data['desiredCreatedAt'],
            $data['activeRunId'] ?? null,
        );
    }
}
