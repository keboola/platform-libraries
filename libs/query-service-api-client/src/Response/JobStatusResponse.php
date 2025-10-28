<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Response;

use Keboola\QueryApi\ClientException;
use Psr\Http\Message\ResponseInterface;

class JobStatusResponse
{
    private const REQUIRED_FIELDS = [
        'queryJobId',
        'status',
        'actorType',
        'changedAt',
        'createdAt',
    ];

    /**
     * @param array<Statement> $statements
     */
    public function __construct(
        readonly string $queryJobId,
        readonly string $status,
        readonly string $actorType,
        readonly string $changedAt,
        readonly string $createdAt,
        readonly array $statements = [],
        readonly ?string $canceledAt = null,
        readonly ?string $cancellationReason = null,
    ) {
    }

    public static function fromResponse(ResponseInterface $response): self
    {
        $data = ResponseParser::parseResponse($response);

        foreach (self::REQUIRED_FIELDS as $field) {
            if (!isset($data[$field]) || !is_string($data[$field])) {
                throw new ClientException("Invalid response: missing or invalid $field");
            }
        }

        /** @var array{
         *     queryJobId: string,
         *     status: string,
         *     actorType: string,
         *     changedAt: string,
         *     createdAt: string,
         *     canceledAt?: string,
         *     cancellationReason?: string,
         *     statements?: array<array<string, mixed>>
         * } $data
         */

        $statements = $data['statements'] ?? [];
        // Type is already known from PHPDoc
        if (!is_array($statements)) { // @phpstan-ignore-line
            $statements = [];
        }

        $statementObjects = [];
        /** @var array<array<string, mixed>> $statements */
        foreach ($statements as $statementData) {
            $statementObjects[] = new Statement($statementData);
        }

        return new self(
            $data['queryJobId'],
            $data['status'],
            $data['actorType'],
            $data['changedAt'],
            $data['createdAt'],
            $statementObjects,
            $data['canceledAt'] ?? null,
            $data['cancellationReason'] ?? null,
        );
    }

    public function getActorType(): string
    {
        return $this->actorType;
    }

    public function getCanceledAt(): ?string
    {
        return $this->canceledAt;
    }

    public function getCancellationReason(): ?string
    {
        return $this->cancellationReason;
    }

    public function getChangedAt(): string
    {
        return $this->changedAt;
    }

    public function getCreatedAt(): string
    {
        return $this->createdAt;
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
     * @return array<Statement>
     */
    public function getStatements(): array
    {
        return $this->statements;
    }
}
