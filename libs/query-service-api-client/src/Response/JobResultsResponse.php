<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Response;

use Keboola\QueryApi\ClientException;
use Psr\Http\Message\ResponseInterface;

class JobResultsResponse
{
    private const REQUIRED_FIELDS = ['data', 'status', 'numberOfRows', 'rowsAffected'];

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

    public static function fromResponse(ResponseInterface $response): self
    {
        $data = ResponseParser::parseResponse($response);

        foreach (self::REQUIRED_FIELDS as $field) {
            if (!isset($data[$field])) {
                throw new ClientException("Invalid response: missing $field");
            }
        }

        /** @var array{
         *     columns?: array<array<string, mixed>>,
         *     message?: string,
         *     numberOfRows: int,
         *     rowsAffected: int,
         *     status: string,
         *     data: array<array<mixed>>
         * } $data
         */

        return new self(
            $data['status'],
            $data['numberOfRows'],
            $data['rowsAffected'],
            $data['data'],
            $data['columns'] ?? [],
            $data['message'] ?? null,
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
