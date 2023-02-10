<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

use DateTimeImmutable;
use Keboola\AzureApiClient\Exception\InvalidResponseException;
use Keboola\AzureApiClient\ResponseModelInterface;

final class TokenResponse implements ResponseModelInterface
{
    public function __construct(
        /** @var non-empty-string $accessToken */
        public readonly string $accessToken,
        public readonly DateTimeImmutable $accessTokenExpiration,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        if (empty($data['access_token']) || !is_string($data['access_token'])) {
            throw new InvalidResponseException('Missing or invalid "access_token" in response: ' . json_encode($data));
        }

        if (empty($data['expires_in']) || !is_numeric($data['expires_in'])) {
            throw new InvalidResponseException('Missing or invalid "expires_in" in response: ' . json_encode($data));
        }

        return new self(
            $data['access_token'],
            new DateTimeImmutable(sprintf('+ %s second', $data['expires_in'])),
        );
    }
}
