<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Response;

use InvalidArgumentException;

class JobResultsResponse
{
    /**
     * @param array<int, array{name: string, type: string}> $columns
     * @param array<array<int, string>> $data
     */
    public function __construct(
        private readonly array $columns,
        private readonly array $data,
        private readonly string $status,
        private readonly int $rowsAffected,
    ) {
    }

    /**
     * @return array<int, array{name: string, type: string}>
     */
    public function getColumns(): array
    {
        return $this->columns;
    }

    /**
     * @return array<array<int, string>>
     */
    public function getData(): array
    {
        return $this->data;
    }

    public function getStatus(): string
    {
        return $this->status;
    }

    public function getRowsAffected(): int
    {
        return $this->rowsAffected;
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        if (!isset($data['status']) || !is_string($data['status'])) {
            throw new InvalidArgumentException('Invalid response: missing status');
        }

        /** @var array<int, array{name: string, type: string}> $columns */
        $columns = $data['columns'] ?? [];

        /** @var array<array<int, string>> $resultData */
        $resultData = $data['data'] ?? [];

        $rowsAffected = isset($data['rowsAffected']) && is_int($data['rowsAffected'])
            ? $data['rowsAffected']
            : 0;

        return new self(
            $columns,
            $resultData,
            $data['status'],
            $rowsAffected,
        );
    }
}
