<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient;

use Keboola\ApiClientBase\ErrorMessageResolverInterface;

final class GitServiceErrorMessageResolver implements ErrorMessageResolverInterface
{
    public function __invoke(string $responseBody, int $statusCode): ?string
    {
        $data = json_decode($responseBody, true);
        if (!is_array($data)) {
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
