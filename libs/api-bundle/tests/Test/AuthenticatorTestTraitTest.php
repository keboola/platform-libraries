<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Test;

use Keboola\ApiBundle\DependencyInjection\KeboolaApiExtension;
use Keboola\ApiBundle\Security\ApplicationToken\ManageApiClientFactory;
use Keboola\ApiBundle\Test\AuthenticatorTestTrait;
use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\StorageApiBranch\Factory\AuthType;
use Keboola\StorageApiBranch\Factory\StorageClientRequestFactory;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;
use Symfony\Component\DependencyInjection\Exception\InvalidArgumentException;

class AuthenticatorTestTraitTest extends WebTestCase
{
    use AuthenticatorTestTrait;

    protected function tearDown(): void
    {
        parent::tearDown();

        // Booting the kernel registers an exception handler that PHPUnit otherwise reports as a
        // leaked handler ("did not remove its own exception handlers"); restore it here.
        restore_exception_handler();
    }

    public function testSetupFakeStorageApiToken(): void
    {
        $token = $this->setupFakeStorageApiToken(
            tokenString: 'my-token',
            projectId: '456',
            features: ['feat-a'],
            adminId: '99',
        );

        self::assertSame('my-token', $token->getTokenValue());
        self::assertSame('456', $token->getProjectId());
        self::assertSame(['feat-a'], $token->getFeatures());
        self::assertSame(AuthType::STORAGE_TOKEN, $token->getTokenType());

        self::assertInstanceOf(
            StorageClientRequestFactory::class,
            self::getContainer()->get(StorageClientRequestFactory::class),
        );
    }

    public function testSetupFakeOAuthToken(): void
    {
        $token = $this->setupFakeOAuthToken(
            tokenString: 'oauth-token',
            projectId: '456',
            features: ['feat-a'],
            adminId: '99',
        );

        self::assertSame('oauth-token', $token->getTokenValue());
        self::assertSame('456', $token->getProjectId());
        self::assertSame(['feat-a'], $token->getFeatures());
        // An OAuth request resolves to a bearer-typed token, so a Storage client built from it
        // authenticates with the bearer scheme instead of X-StorageApi-Token.
        self::assertSame(AuthType::BEARER, $token->getTokenType());

        self::assertInstanceOf(
            StorageClientRequestFactory::class,
            self::getContainer()->get(StorageClientRequestFactory::class),
        );
    }

    public function testSetupFakeManageApiToken(): void
    {
        $this->setupFakeManageApiToken('manage-token', ['some:scope'], ['feat-b']);

        $factory = self::getContainer()->get(ManageApiClientFactory::class);
        self::assertInstanceOf(ManageApiClientFactory::class, $factory);

        $data = $factory->getClientForManageToken('manage-token')->verifyToken();
        self::assertSame(['some:scope'], $data['scopes']);
        self::assertArrayHasKey('user', $data);
        self::assertSame(['feat-b'], $data['user']['features']);

        // The Kubernetes ServiceAccount JWT path is stubbed identically.
        $jwtData = $factory->getClientForServiceAccountToken('manage-token')->verifyToken();
        self::assertSame(['some:scope'], $jwtData['scopes']);
    }

    public function testSetupFakeConnectionToken(): void
    {
        $token = $this->setupFakeConnectionToken(
            projectId: '789',
            features: ['feat-x'],
            tokenString: 'tok-x',
            adminId: '7',
        );

        // The returned StorageApiToken carries the values passed in.
        self::assertSame('tok-x', $token->getTokenValue());
        self::assertSame('789', $token->getProjectId());
        self::assertSame(['feat-x'], $token->getFeatures());

        // A resolver ManageApiClient mock is registered in the container under the resolver id.
        $resolverClient = self::getContainer()->get(KeboolaApiExtension::STORAGE_TOKEN_RESOLVER_CLIENT_ID);
        self::assertInstanceOf(ManageApiClient::class, $resolverClient);

        // The mock resolver client returns the legacy Storage token together with its full
        // detail, matching the returned StorageApiToken - no Storage API stub is involved.
        // tokenDetail is not in the released client's return shape yet (only on its default
        // branch), so override the type the same way StorageApiTokenFactory does.
        /** @var array<string, mixed> $resolved */
        $resolved = $resolverClient->resolveStorageToken(789, 'kbc_at_x');
        self::assertSame(789, $resolved['projectId']);
        self::assertSame('tok-x', $resolved['storageToken']);
        self::assertSame($token->getTokenInfo(), $resolved['tokenDetail']);
    }

    public function testStubbingInitializedManageFactoryFailsWithoutCleanClient(): void
    {
        // A #[StorageApiTokenAuth] request initializes ManageApiClientFactory (it backs the token
        // exchange resolver); once initialized the test container can no longer replace it.
        self::getContainer()->get(ManageApiClientFactory::class);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/already initialized/');

        $this->setupFakeManageApiToken('manage-token');
    }

    public function testBootCleanClientAllowsStubbingAfterServiceInitialized(): void
    {
        // Initialize ManageApiClientFactory on the current container, as a #[StorageApiTokenAuth]
        // request would.
        self::getContainer()->get(ManageApiClientFactory::class);

        // A fresh client gives a clean container where the factory can be stubbed again.
        self::bootCleanClient();
        $this->setupFakeManageApiToken('manage-token', ['some:scope']);

        $factory = self::getContainer()->get(ManageApiClientFactory::class);
        self::assertInstanceOf(ManageApiClientFactory::class, $factory);
        self::assertSame(
            ['some:scope'],
            $factory->getClientForManageToken('manage-token')->verifyToken()['scopes'],
        );
    }
}
