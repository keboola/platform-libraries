<?php

declare(strict_types=1);

namespace Keboola\QueryApi;

use JsonException;
use Keboola\ApiClientBase\ErrorMessageResolverInterface;
use Keboola\ApiClientBase\Json;

final class QueryApiErrorMessageResolver implements ErrorMessageResolverInterface
{
    public function __invoke(string $responseBody, int $statusCode): ?string
    {
        try {
            $data = Json::decodeArray($responseBody);
        } catch (JsonException) {
            return null;
        }

        $message = $data['exception'] ?? null;
        if (!is_string($message) || trim($message) === '') {
            return null;
        }

        return trim($message);
    }
}
