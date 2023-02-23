<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

use Keboola\AzureApiClient\Authentication\Authenticator\AuthenticatorInterface;
use Psr\Http\Message\RequestInterface;

interface AuthorizationHeaderResolverInterface
{
    public function __construct(
        AuthenticatorInterface $authenticator,
        string $resource
    );

    public function __invoke(RequestInterface $request): RequestInterface;
}
