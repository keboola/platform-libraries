<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\ApiClientFactory;

use Keboola\AzureApiClient\Authentication\AuthenticatorFactory;
use Keboola\AzureApiClient\Authentication\AuthenticatorInterface;
use Keboola\AzureApiClient\Authentication\TokenInterface;
use Keboola\AzureApiClient\Authentication\TokenWithExpiration;
use Psr\Http\Message\RequestInterface;

class AegSasKeyAuthorizationHeaderResolver implements AuthorizationHeaderResolverInterface
{
    private ?TokenInterface $token = null;

    public function __construct(
        private readonly AuthenticatorInterface $authenticator,
        private readonly string $resource
    ) {
    }

    public function __invoke(RequestInterface $request): RequestInterface
    {
        if (!$this->isTokenValid()) {
            $this->refreshToken();
        }
        assert($this->token !== null);

        return $request->withHeader('aeg-sas-key', $this->token);
    }

    private function isTokenValid(): bool
    {
        if ($this->token === null) {
            return false;
        }

        return $this->token->isValid();
    }

    private function refreshToken(): void
    {
        $this->token = $this->authenticator->getAuthenticationToken($this->resource);
    }
}
