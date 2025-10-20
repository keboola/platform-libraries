<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Response;

use InvalidArgumentException;

class QueryJobResponse
{
    public function __construct(
        private readonly string $queryJobId,
    ) {
    }

    public function getQueryJobId(): string
    {
        return $this->queryJobId;
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        if (!isset($data['queryJobId']) || !is_string($data['queryJobId'])) {
            throw new InvalidArgumentException('Invalid response: missing queryJobId');
        }

        return new self($data['queryJobId']);
    }
}
