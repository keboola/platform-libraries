<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient\Model;

use Keboola\ApiClientBase\ResponseModelInterface;
use Webmozart\Assert\Assert;

final readonly class Deployment implements ResponseModelInterface
{
    public const BUILD_STATE_PENDING = 'pending';
    public const BUILD_STATE_BUILDING = 'building';
    public const BUILD_STATE_READY = 'ready';
    public const BUILD_STATE_FAILED = 'failed';

    public const RUN_STATE_NEVER_RUN = 'neverRun';
    public const RUN_STATE_PENDING = 'pending';
    public const RUN_STATE_STARTING = 'starting';
    public const RUN_STATE_RUNNING = 'running';
    public const RUN_STATE_STOPPING = 'stopping';
    public const RUN_STATE_STOPPED = 'stopped';
    public const RUN_STATE_FAILED = 'failed';

    /**
     * @param string $buildState One of the BUILD_STATE_* constants, kept a plain string like $runState.
     * @param string $runState One of the RUN_STATE_* constants. It is a plain string, so a state added by the
     *     API does not break the response mapping.
     */
    public function __construct(
        public string $id,
        public string $configVersion,
        public ?string $gitCommitSha,
        public ?string $runtimeSize,
        public string $buildState,
        public string $runState,
        public bool $archived,
        public ?string $archivedAt,
        public string $createdAt,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        Assert::keyExists($data, 'id');
        Assert::keyExists($data, 'configVersion');
        Assert::keyExists($data, 'gitCommitSha');
        Assert::keyExists($data, 'buildState');
        Assert::keyExists($data, 'runState');
        Assert::keyExists($data, 'archived');
        Assert::keyExists($data, 'archivedAt');
        Assert::keyExists($data, 'createdAt');
        Assert::string($data['id']);
        Assert::string($data['configVersion']);
        Assert::nullOrString($data['gitCommitSha']);
        Assert::nullOrString($data['runtimeSize'] ?? null);
        Assert::string($data['buildState']);
        Assert::string($data['runState']);
        Assert::boolean($data['archived']);
        Assert::nullOrString($data['archivedAt']);
        Assert::string($data['createdAt']);

        return new self(
            $data['id'],
            $data['configVersion'],
            $data['gitCommitSha'],
            $data['runtimeSize'] ?? null,
            $data['buildState'],
            $data['runState'],
            $data['archived'],
            $data['archivedAt'],
            $data['createdAt'],
        );
    }
}
