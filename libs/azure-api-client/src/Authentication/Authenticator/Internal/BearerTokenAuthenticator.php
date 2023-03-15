<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator\Internal;

use Keboola\AzureApiClient\Authentication\AuthenticationToken;
use Keboola\AzureApiClient\Authentication\Authenticator\RequestAuthenticatorInterface;
use Psr\Http\Message\RequestInterface;

class BearerTokenAuthenticator implements RequestAuthenticatorInterface
{
    private const EXPIRATION_MARGIN = 60; // seconds

    private ?AuthenticationToken $token = null;

    public function __construct(
        private readonly BearerTokenResolver $tokenResolver,
        private readonly string $resource,
    ) {
    }

    public function __invoke(RequestInterface $request): RequestInterface
    {
        if ($this->token === null || !$this->isTokenValid($this->token)) {
            $this->token = $this->tokenResolver->getAuthenticationToken($this->resource);
        }

        return $request->withHeader('Authorization', 'Bearer ' . $this->token->value);
    }

    private function isTokenValid(AuthenticationToken $token): bool
    {
        if ($token->expiresAt === null) {
            return true;
        }

        $expirationTimestamp = $token->expiresAt->getTimestamp();
        if ($expirationTimestamp - self::EXPIRATION_MARGIN < time()) {
            return false;
        }

        return true;
    }
}
