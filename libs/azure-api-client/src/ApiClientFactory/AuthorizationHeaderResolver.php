<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\ApiClientFactory;

use Keboola\AzureApiClient\Authentication\AuthenticatorFactory;
use Keboola\AzureApiClient\Authentication\AuthenticatorInterface;
use Keboola\AzureApiClient\Authentication\TokenResponse;
use Psr\Http\Message\RequestInterface;

class AuthorizationHeaderResolver
{
    private const EXPIRATION_MARGIN = 60; // seconds

    private ?AuthenticatorInterface $authenticator = null;
    private ?TokenResponse $token = null;

    public function __construct(
        private readonly AuthenticatorFactory $authenticatorFactory,
        private readonly string $resource
    ) {
    }

    public function __invoke(RequestInterface $request): RequestInterface
    {
        if (!$this->isTokenValid()) {
            $this->refreshToken();
        }
        assert($this->token !== null);

        return $request->withHeader('Authorization', 'Bearer ' . $this->token->accessToken);
    }

    private function isTokenValid(): bool
    {
        if ($this->token === null) {
            return false;
        }

        $expirationTimestamp = $this->token->accessTokenExpiration->getTimestamp();
        if ($expirationTimestamp - self::EXPIRATION_MARGIN < time()) {
            return false;
        }

        return true;
    }

    private function refreshToken(): void
    {
        if ($this->authenticator === null) {
            $this->authenticator = $this->authenticatorFactory->createAuthenticator();
        }

        $this->token = $this->authenticator->getAuthenticationToken($this->resource);
    }
}
