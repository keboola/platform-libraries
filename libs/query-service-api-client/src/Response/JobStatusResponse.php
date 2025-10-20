<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Response;

use InvalidArgumentException;

class JobStatusResponse
{
    /**
     * @param array<array<string, mixed>> $statements
     * @param array<string, mixed> $rawData
     */
    public function __construct(
        private readonly string $queryJobId,
        private readonly string $status,
        private readonly array $statements,
        private readonly ?string $cancellationReason,
        private readonly ?string $canceledAt,
        private readonly array $rawData,
    ) {
    }

    public function getQueryJobId(): string
    {
        return $this->queryJobId;
    }

    public function getStatus(): string
    {
        return $this->status;
    }

    /**
     * @return array<array<string, mixed>>
     */
    public function getStatements(): array
    {
        return $this->statements;
    }

    public function getCancellationReason(): ?string
    {
        return $this->cancellationReason;
    }

    public function getCanceledAt(): ?string
    {
        return $this->canceledAt;
    }

    /**
     * @return array<string, mixed>
     */
    public function getRawData(): array
    {
        return $this->rawData;
    }

    public function isCompleted(): bool
    {
        return $this->status === 'completed';
    }

    public function isFailed(): bool
    {
        return $this->status === 'failed';
    }

    public function isCanceled(): bool
    {
        return $this->status === 'canceled';
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        if (!isset($data['queryJobId']) || !is_string($data['queryJobId'])) {
            throw new InvalidArgumentException('Invalid response: missing queryJobId');
        }

        if (!isset($data['status']) || !is_string($data['status'])) {
            throw new InvalidArgumentException('Invalid response: missing status');
        }

        /** @var array<array<string, mixed>> $statements */
        $statements = $data['statements'] ?? [];

        return new self(
            $data['queryJobId'],
            $data['status'],
            $statements,
            isset($data['cancellationReason']) && is_string($data['cancellationReason'])
                ? $data['cancellationReason']
                : null,
            isset($data['canceledAt']) && is_string($data['canceledAt'])
                ? $data['canceledAt']
                : null,
            $data,
        );
    }
}
