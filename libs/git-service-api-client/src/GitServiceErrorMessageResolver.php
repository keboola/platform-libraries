<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient;

use JsonException;
use Keboola\ApiClientBase\ErrorMessageResolverInterface;
use Keboola\ApiClientBase\Json;

final class GitServiceErrorMessageResolver implements ErrorMessageResolverInterface
{
    public function __invoke(string $responseBody, int $statusCode): ?string
    {
        try {
            $data = Json::decodeArray($responseBody);
        } catch (JsonException) {
            return null;
        }

        $code = $data['code'] ?? null;
        $error = $data['error'] ?? null;
        if (!is_string($code) || !is_string($error) || $code === '' || $error === '') {
            return null;
        }

        return trim($code . ': ' . $error);
    }
}
