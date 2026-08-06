<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator;

use Keboola\AzureApiClient\Authentication\Authenticator\Internal\SASTokenAuthenticator;

class SASTokenAuthenticatorFactory implements RequestAuthenticatorFactoryInterface
{
    public function __construct(
        private readonly string $url,
        private readonly string $sharedAccessKeyName,
        private readonly string $sharedAccessKey
    ) {
    }

    public function createRequestAuthenticator(string $resource): RequestAuthenticatorInterface
    {
        return new SASTokenAuthenticator(
            url: $this->url,
            sharedAccessKeyName: $this->sharedAccessKeyName,
            sharedAccessKey: $this->sharedAccessKey
        );
    }
}
