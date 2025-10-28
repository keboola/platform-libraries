<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Response;

use JsonException;
use Keboola\QueryApi\ClientException;
use Psr\Http\Message\ResponseInterface;

class ResponseParser
{
    /**
     * @return array<string, mixed>
     */
    public static function parseResponse(ResponseInterface $response): array
    {
        $body = (string) $response->getBody();

        if (empty($body)) {
            return [];
        }

        try {
            $data = json_decode($body, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new ClientException('Response is not valid JSON: ' . $e->getMessage(), 0, $e);
        }

        if (!is_array($data)) {
            throw new ClientException('Response is not a JSON object');
        }

        /** @var array<string, mixed> $data */
        return $data;
    }
}
