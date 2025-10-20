<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Response;

use InvalidArgumentException;

class HealthCheckResponse
{
    /**
     * @param array<string, mixed> $rawData
     */
    public function __construct(
        private readonly string $status,
        private readonly array $rawData,
    ) {
    }

    public function getStatus(): string
    {
        return $this->status;
    }

    /**
     * @return array<string, mixed>
     */
    public function getRawData(): array
    {
        return $this->rawData;
    }

    public function isOk(): bool
    {
        return $this->status === 'ok';
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        if (!isset($data['status']) || !is_string($data['status'])) {
            throw new InvalidArgumentException('Invalid response: missing status');
        }

        return new self($data['status'], $data);
    }
}
