<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator;

use Keboola\AzureApiClient\Authentication\Authenticator\Internal\SharedAccessSignatureAuthenticator;
use Webmozart\Assert\Assert;

/**
 * Shared Access Signature authentication for Service Bus (and other services using the same SAS scheme).
 * https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-sas
 */
class SharedAccessSignatureAuth implements RequestAuthenticatorFactoryInterface
{
    private const DEFAULT_TOKEN_TTL_SECONDS = 3600;

    /**
     * @param non-empty-string $keyName Name of the shared access policy, e.g. "RootManageSharedAccessKey".
     * @param non-empty-string $key Primary or secondary key of the policy.
     * @param positive-int $tokenTtlSeconds How long each signed token stays valid.
     */
    public function __construct(
        private readonly string $keyName,
        private readonly string $key,
        private readonly int $tokenTtlSeconds = self::DEFAULT_TOKEN_TTL_SECONDS,
    ) {
        Assert::stringNotEmpty($this->keyName);
        Assert::stringNotEmpty($this->key);
        Assert::greaterThan($this->tokenTtlSeconds, 0);
    }

    public function createRequestAuthenticator(string $resource): RequestAuthenticatorInterface
    {
        return new SharedAccessSignatureAuthenticator($this->keyName, $this->key, $this->tokenTtlSeconds);
    }
}
