<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient\Model;

use Keboola\ApiClientBase\ResponseModelInterface;

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
     * @param non-empty-string $configVersion
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
        /**
         * @var array{
         *     id: string,
         *     configVersion: non-empty-string,
         *     gitCommitSha: string|null,
         *     runtimeSize?: string|null,
         *     buildState: string,
         *     runState: string,
         *     archived: bool,
         *     archivedAt: string|null,
         *     createdAt: string,
         * } $data
         */
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
