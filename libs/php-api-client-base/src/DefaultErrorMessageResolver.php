<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase;

final class DefaultErrorMessageResolver implements ErrorMessageResolverInterface
{
    public function __invoke(string $responseBody, int $statusCode): ?string
    {
        $data = json_decode($responseBody, true);
        if (!is_array($data)) {
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
