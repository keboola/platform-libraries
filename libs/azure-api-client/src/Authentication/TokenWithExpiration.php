<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

use DateTimeImmutable;
use Keboola\AzureApiClient\Exception\InvalidResponseException;
use Keboola\AzureApiClient\ResponseModelInterface;

final class TokenWithExpiration implements ResponseModelInterface, TokenInterface
{
    public function __construct(
        /** @var non-empty-string $accessToken */
        private readonly string $accessToken,
        private readonly DateTimeImmutable $accessTokenExpiration,
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

    public function getToken(): string
    {
        return $this->accessToken;
    }

    public function isValid(): bool
    {
        $expirationTimestamp = $this->accessTokenExpiration->getTimestamp();
        if ($expirationTimestamp - self::EXPIRATION_MARGIN < time()) {
            return false;
        }

        return true;
    }
}
