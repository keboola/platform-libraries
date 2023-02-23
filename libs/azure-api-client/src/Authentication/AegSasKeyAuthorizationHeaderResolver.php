<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

use Keboola\AzureApiClient\Authentication\Authenticator\AuthenticatorInterface;
use Psr\Http\Message\RequestInterface;

class AegSasKeyAuthorizationHeaderResolver implements AuthorizationHeaderResolverInterface
{
    private ?AuthenticationToken $token = null;

    public function __construct(
        private readonly AuthenticatorInterface $authenticator,
        private readonly string $resource
    ) {
    }

    public function __invoke(RequestInterface $request): RequestInterface
    {
        $this->token ??= $this->authenticator->getAuthenticationToken($this->resource);

        return $request->withHeader('aeg-sas-key', $this->token->value);
    }
}
