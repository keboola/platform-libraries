<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\StorageApiClient;

use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\AuthType;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use SensitiveParameter;
use Symfony\Component\HttpFoundation\Request;

/**
 * Builds the Storage {@see ClientWrapper} used to verify a token carried by an incoming request.
 *
 * Bundle-owned successor to keboola/storage-api-php-client-branch-wrapper's
 * {@see \Keboola\StorageApiBranch\Factory\StorageClientRequestFactory}: the request's token and
 * auth type are resolved once by
 * {@see \Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenAuthenticator} and passed in,
 * so this factory only binds them onto the bundle's base {@see ClientOptions} (the same base the
 * controller-facing {@see StorageClientApiFactory} uses). Kept injectable (non-final) so functional
 * tests can replace it via {@see \Keboola\ApiBundle\Test\AuthenticatorTestTrait}.
 */
class RequestStorageClientFactory
{
    public function __construct(
        private readonly ClientOptions $baseClientOptions,
    ) {
    }

    public function createClientWrapper(
        #[SensitiveParameter]
        string $token,
        AuthType $authType,
        Request $request,
        ?ClientOptions $overrides = null,
    ): ClientWrapper {
        return StorageClientWrapperFactory::create(
            $this->baseClientOptions,
            $token,
            $authType,
            $request,
            $overrides,
        );
    }
}
