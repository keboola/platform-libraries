<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient\Model;

use Keboola\ApiClientBase\ResponseModelInterface;

final readonly class Run implements ResponseModelInterface
{
    public const STATE_STARTING = 'starting';
    public const STATE_RUNNING = 'running';
    public const STATE_FAILED = 'failed';
    public const STATE_FINISHED = 'finished';

    public function __construct(
        public string $id,
        public string $appId,
        public ?string $deploymentId,
        public ?string $state,
        public ?string $createdAt,
        public ?string $startedAt,
        public ?string $stoppedAt,
        public ?RunFailureReason $failureReason,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        /**
         * @var array{
         *     id: string,
         *     appId: string,
         *     deploymentId: string|null,
         *     state: string|null,
         *     createdAt: string|null,
         *     startedAt: string|null,
         *     stoppedAt: string|null,
         *     failureReason: array<mixed>|null,
         * } $data
         */
        return new self(
            $data['id'],
            $data['appId'],
            $data['deploymentId'],
            $data['state'],
            $data['createdAt'],
            $data['startedAt'],
            $data['stoppedAt'],
            $data['failureReason'] === null ? null : RunFailureReason::fromResponseData($data['failureReason']),
        );
    }
}
