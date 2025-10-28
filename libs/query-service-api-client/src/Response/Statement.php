<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Response;

use Keboola\QueryApi\ClientException;

/**
 * Represents a single statement within a query job.
 */
class Statement
{
    private const REQUIRED_FIELDS = [
        'id', 'query', 'status',
    ];

    private string $id;
    private ?string $queryId;
    private ?string $sessionId;
    private string $query;
    private string $status;
    private ?int $numberOfRows;
    private ?int $rowsAffected;
    private ?string $executedAt;
    private ?string $completedAt;
    private ?string $createdAt;
    private ?string $error;

    /**
     * @param array<string, mixed> $data
     */
    public function __construct(array $data)
    {
        foreach (self::REQUIRED_FIELDS as $field) {
            if (!isset($data[$field])) {
                throw new ClientException("Invalid statement response: missing $field");
            }
        }

        /** @var array{
         *     id: string,
         *     queryId?: string,
         *     sessionId?: string,
         *     query: string,
         *     status: string,
         *     numberOfRows?: int,
         *     rowsAffected?: int,
         *     executedAt?: string,
         *     completedAt?: string,
         *     createdAt?: string,
         *     error?: string
         * } $data
         */
        $this->id = $data['id'];
        $this->queryId = $data['queryId'] ?? null;
        $this->sessionId = $data['sessionId'] ?? null;
        $this->query = $data['query'];
        $this->status = $data['status'];
        $this->numberOfRows = $data['numberOfRows'] ?? null;
        $this->rowsAffected = $data['rowsAffected'] ?? null;
        $this->executedAt = $data['executedAt'] ?? null;
        $this->completedAt = $data['completedAt'] ?? null;
        $this->createdAt = $data['createdAt'] ?? null;
        $this->error = $data['error'] ?? null;
    }

    public function getCompletedAt(): ?string
    {
        return $this->completedAt;
    }

    public function getCreatedAt(): ?string
    {
        return $this->createdAt;
    }

    public function getError(): ?string
    {
        return $this->error;
    }

    public function getExecutedAt(): ?string
    {
        return $this->executedAt;
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function getNumberOfRows(): ?int
    {
        return $this->numberOfRows;
    }

    public function getQuery(): string
    {
        return $this->query;
    }

    public function getQueryId(): ?string
    {
        return $this->queryId;
    }

    public function getRowsAffected(): ?int
    {
        return $this->rowsAffected;
    }

    public function getSessionId(): ?string
    {
        return $this->sessionId;
    }

    public function getStatus(): string
    {
        return $this->status;
    }
}
