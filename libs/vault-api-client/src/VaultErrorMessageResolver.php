<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient;

use JsonException;
use Keboola\ApiClientBase\ErrorMessageResolverInterface;
use Keboola\ApiClientBase\Json;

final class VaultErrorMessageResolver implements ErrorMessageResolverInterface
{
    public function __invoke(string $responseBody, int $statusCode): ?string
    {
        try {
            $data = Json::decodeArray($responseBody);
        } catch (JsonException) {
            return null;
        }

        if (empty($data['error']) || empty($data['code'])) {
            return null;
        }

        return trim($data['code'] . ': ' . $data['error']);
    }
}
