<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Response;

use Keboola\QueryApi\ClientException;
use Psr\Http\Message\ResponseInterface;

class SubmitQueryJobResponse
{
    private const REQUIRED_FIELDS = ['queryJobId'];

    public function __construct(readonly string $queryJobId)
    {
    }

    public static function fromResponse(ResponseInterface $response): self
    {
        $data = ResponseParser::parseResponse($response);

        foreach (self::REQUIRED_FIELDS as $field) {
            if (!isset($data[$field]) || !is_string($data[$field])) {
                throw new ClientException("Invalid response: missing or invalid $field");
            }
        }

        return new self($data['queryJobId']);
    }

    public function getQueryJobId(): string
    {
        return $this->queryJobId;
    }
}
