<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient;

use JsonException;
use Keboola\ApiClientBase\ErrorMessageResolverInterface;
use Keboola\ApiClientBase\Json;

final class SyncActionsErrorMessageResolver implements ErrorMessageResolverInterface
{
    public function __invoke(string $responseBody, int $statusCode): ?string
    {
        try {
            $data = Json::decodeArray($responseBody);
        } catch (JsonException) {
            return null;
        }

        $error = $data['error'] ?? null;
        $code = $data['code'] ?? null;

        if (empty($error) || empty($code) || !is_scalar($error) || !is_scalar($code)) {
            return null;
        }

        return trim((string) $code . ': ' . (string) $error);
    }
}
