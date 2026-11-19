<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient\Model;

use Keboola\ApiClientBase\ResponseModelInterface;
use Webmozart\Assert\Assert;

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
        Assert::keyExists($data, 'id');
        Assert::keyExists($data, 'projectId');
        Assert::keyExists($data, 'componentId');
        Assert::keyExists($data, 'branchId');
        Assert::keyExists($data, 'configId');
        Assert::keyExists($data, 'url');
        Assert::keyExists($data, 'hasManagedGitRepo');
        Assert::keyExists($data, 'production');
        Assert::keyExists($data, 'state');
        Assert::string($data['id']);
        Assert::string($data['projectId']);
        Assert::string($data['componentId']);
        Assert::nullOrString($data['branchId']);
        Assert::string($data['configId']);
        Assert::nullOrString($data['type'] ?? null);
        Assert::nullOrString($data['url']);
        Assert::boolean($data['hasManagedGitRepo']);
        Assert::isArray($data['production']);
        Assert::string($data['state']);

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
