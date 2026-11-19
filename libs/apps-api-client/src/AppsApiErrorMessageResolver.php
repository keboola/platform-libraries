<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient;

use JsonException;
use Keboola\ApiClientBase\ErrorMessageResolverInterface;
use Keboola\ApiClientBase\Json;

final class AppsApiErrorMessageResolver implements ErrorMessageResolverInterface
{
    public function __invoke(string $responseBody, int $statusCode): ?string
    {
        try {
            $data = Json::decodeArray($responseBody);
        } catch (JsonException) {
            return null;
        }

        $error = $data['error'] ?? null;
        if (!is_string($error) || $error === '') {
            return null;
        }

        return $error;
    }
}
