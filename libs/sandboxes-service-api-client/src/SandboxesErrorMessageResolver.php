<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient;

use JsonException;
use Keboola\ApiClientBase\ErrorMessageResolverInterface;
use Keboola\ApiClientBase\Json;

final class SandboxesErrorMessageResolver implements ErrorMessageResolverInterface
{
    public function __invoke(string $responseBody, int $statusCode): ?string
    {
        try {
            $data = Json::decodeArray($responseBody);
        } catch (JsonException) {
            return null;
        }

        if (isset($data['error'], $data['message'])
            && is_string($data['error']) && $data['error'] !== ''
            && is_string($data['message']) && $data['message'] !== ''
        ) {
            return trim(sprintf('%s: %s', $data['error'], $data['message']));
        }

        return null;
    }
}
