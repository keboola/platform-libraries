<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient\Authentication;

use Psr\Http\Message\RequestInterface;

interface RequestAuthenticatorInterface
{
    public function __invoke(RequestInterface $request): RequestInterface;
}
