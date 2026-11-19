<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient\Model;

use Keboola\ApiClientBase\ResponseModelInterface;

final readonly class App implements ResponseModelInterface
{
    public const STATE_NOT_DEPLOYED = 'notDeployed';
    public const STATE_STARTING = 'starting';
    public const STATE_RUNNING = 'running';
    public const STATE_SUSPENDED = 'suspended';
    public const STATE_DISABLED = 'disabled';

    public function __construct(
        public string $id,
        public string $projectId,
        public string $componentId,
        public ?string $branchId,
        public string $configId,
        public ?string $type,
        public ?string $url,
        public bool $hasManagedGitRepo,
        public AppProduction $production,
        public string $state,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        /**
         * @var array{
         *     id: string,
         *     projectId: string,
         *     componentId: string,
         *     branchId: string|null,
         *     configId: string,
         *     type?: string|null,
         *     url: string|null,
         *     hasManagedGitRepo: bool,
         *     production: array<mixed>,
         *     state: string,
         * } $data
         */
        return new self(
            $data['id'],
            $data['projectId'],
            $data['componentId'],
            $data['branchId'],
            $data['configId'],
            $data['type'] ?? null,
            $data['url'],
            $data['hasManagedGitRepo'],
            AppProduction::fromResponseData($data['production']),
            $data['state'],
        );
    }
}
