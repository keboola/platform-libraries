<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Response;

use Keboola\ApiClientBase\ResponseModelInterface;
use Psr\Http\Message\ResponseInterface;
use Webmozart\Assert\Assert;

final class JobResultsResponse implements ResponseModelInterface
{
    /**
     * @param array<array<string, mixed>> $columns
     * @param array<array<mixed>> $data
     */
    public function __construct(
        readonly string $status,
        readonly int $numberOfRows,
        readonly int $rowsAffected,
        readonly array $data,
        readonly array $columns = [],
        readonly ?string $message = null,
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
        Assert::keyExists($data, 'status');
        Assert::string($data['status']);
        Assert::keyExists($data, 'numberOfRows');
        Assert::integer($data['numberOfRows']);
        Assert::keyExists($data, 'rowsAffected');
        Assert::integer($data['rowsAffected']);
        Assert::keyExists($data, 'data');
        Assert::isArray($data['data']);

        $columns = $data['columns'] ?? [];
        Assert::isArray($columns);
        $message = $data['message'] ?? null;
        Assert::nullOrString($message);

        /** @var array<array<mixed>> $rows */
        $rows = $data['data'];
        /** @var array<array<string, mixed>> $columns */

        return new self(
            $data['status'],
            $data['numberOfRows'],
            $data['rowsAffected'],
            $rows,
            $columns,
            $message,
        );
    }

    /**
     * @return array<array<string, mixed>>
     */
    public function getColumns(): array
    {
        return $this->columns;
    }

    /**
     * @return array<array<mixed>>
     */
    public function getData(): array
    {
        return $this->data;
    }

    public function getMessage(): ?string
    {
        return $this->message;
    }

    public function getNumberOfRows(): int
    {
        return $this->numberOfRows;
    }

    public function getRowsAffected(): int
    {
        return $this->rowsAffected;
    }

    public function getStatus(): string
    {
        return $this->status;
    }
}
