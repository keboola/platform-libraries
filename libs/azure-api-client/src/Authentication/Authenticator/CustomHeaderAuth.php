<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator;

use Keboola\AzureApiClient\Authentication\Authenticator\Internal\CustomHeaderAuthenticator;

class CustomHeaderAuth implements RequestAuthenticatorFactoryInterface
{
    public function __construct(
        private readonly string $header,
        private readonly string $value,
    ) {
    }

    public function createRequestAuthenticator(string $resource): RequestAuthenticatorInterface
    {
        return new CustomHeaderAuthenticator($this->header, $this->value);
    }
}
