<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

use Keboola\AzureApiClient\Exception\InvalidResponseException;
use Keboola\AzureApiClient\ResponseModelInterface;

final class TokenResponse implements ResponseModelInterface
{
    public function __construct(
        /** @var non-empty-string $accessToken */
        public readonly string $accessToken,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        if (empty($data['access_token']) || !is_scalar($data['access_token'])) {
            throw new InvalidResponseException('Access token not provided in response: ' . json_encode($data));
        }

        return new self(
            (string) $data['access_token'],
        );
    }
}
