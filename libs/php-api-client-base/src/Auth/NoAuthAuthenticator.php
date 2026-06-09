<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Auth;

use Psr\Http\Message\RequestInterface;

final readonly class NoAuthAuthenticator implements RequestAuthenticatorInterface
{
    public function __invoke(RequestInterface $request): RequestInterface
    {
        return $request;
    }
}
