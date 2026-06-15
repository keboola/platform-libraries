<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase;

use JsonException;

final class DefaultErrorMessageResolver implements ErrorMessageResolverInterface
{
    public function __invoke(string $responseBody, int $statusCode): ?string
    {
        try {
            $data = Json::decodeArray($responseBody);
        } catch (JsonException) {
            return null;
        }

        foreach (['error', 'message'] as $key) {
            if (isset($data[$key]) && is_string($data[$key]) && $data[$key] !== '') {
                return $data[$key];
            }
        }

        return null;
    }
}
