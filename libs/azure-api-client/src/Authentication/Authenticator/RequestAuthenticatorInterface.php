<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator;

use Psr\Http\Message\RequestInterface;

interface RequestAuthenticatorInterface
{
    public function __invoke(RequestInterface $request): RequestInterface;
}
