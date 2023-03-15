<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator\Internal;

use Keboola\AzureApiClient\Authentication\Authenticator\RequestAuthenticatorInterface;
use Psr\Http\Message\RequestInterface;

class CustomHeaderAuthenticator implements RequestAuthenticatorInterface
{
    public function __construct(
        private readonly string $header,
        private readonly string $value,
    ) {
    }

    public function __invoke(RequestInterface $request): RequestInterface
    {
        return $request->withHeader($this->header, $this->value);
    }
}
