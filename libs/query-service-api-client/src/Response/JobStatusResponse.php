<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Response;

use Keboola\ApiClientBase\ResponseModelInterface;
use Psr\Http\Message\ResponseInterface;
use Webmozart\Assert\Assert;

final class JobStatusResponse implements ResponseModelInterface
{
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

    public static function fromResponse(ResponseInterface $response): static
    {
        return static::fromResponseData(ResponseParser::parseResponse($response));
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromResponseData(array $data): static
    {
        Assert::keyExists($data, 'queryJobId');
        Assert::string($data['queryJobId']);
        Assert::keyExists($data, 'status');
        Assert::string($data['status']);
        Assert::keyExists($data, 'actorType');
        Assert::string($data['actorType']);
        Assert::keyExists($data, 'changedAt');
        Assert::string($data['changedAt']);
        Assert::keyExists($data, 'createdAt');
        Assert::string($data['createdAt']);

        $statements = $data['statements'] ?? [];
        Assert::isArray($statements);

        $statementObjects = [];
        /** @var array<array<string, mixed>> $statements */
        foreach ($statements as $statementData) {
            $statementObjects[] = new Statement($statementData);
        }

        $canceledAt = $data['canceledAt'] ?? null;
        Assert::nullOrString($canceledAt);
        $cancellationReason = $data['cancellationReason'] ?? null;
        Assert::nullOrString($cancellationReason);

        return new self(
            $data['queryJobId'],
            $data['status'],
            $data['actorType'],
            $data['changedAt'],
            $data['createdAt'],
            $statementObjects,
            $canceledAt,
            $cancellationReason,
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
