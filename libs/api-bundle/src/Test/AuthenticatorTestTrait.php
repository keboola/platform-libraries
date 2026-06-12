<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Test;

use Keboola\ApiBundle\DependencyInjection\KeboolaApiExtension;
use Keboola\ApiBundle\Security\ApplicationToken\ManageApiClientFactory;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken;
use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\StorageClientRequestFactory;
use PHPUnit\Framework\MockObject\MockObject;
use Symfony\Bundle\FrameworkBundle\KernelBrowser;
use Symfony\Component\BrowserKit\AbstractBrowser;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\HttpKernel\KernelInterface;

/**
 * Helpers for functional WebTestCase tests that stub api-bundle's authenticators, so
 * controllers guarded by #[StorageApiTokenAuth] / #[ApplicationTokenAuth] can be
 * exercised without reaching real Storage/Manage APIs. The consuming test case provides
 * createMock() and getContainer(); consumers that use {@see bootCleanClient()} must extend
 * Symfony's WebTestCase (it calls bootKernel()/getClient()).
 *
 * The setupFake*Token() helpers register their mocks on the current test container. A
 * #[StorageApiTokenAuth] request initializes ManageApiClientFactory (it backs the token
 * exchange resolver), and an initialized service can no longer be replaced via the test
 * container - so call {@see bootCleanClient()} to get a fresh container/client before
 * setupFake*Token() whenever a request has already run in the test.
 */
trait AuthenticatorTestTrait
{
    /**
     * @param class-string $className
     */
    abstract protected function createMock(string $className): MockObject;

    abstract protected static function getContainer(): ContainerInterface;

    /**
     * @param array<string, mixed> $options
     */
    abstract protected static function bootKernel(array $options = []): KernelInterface;

    abstract protected static function getClient(?AbstractBrowser $newClient = null): ?AbstractBrowser;

    /**
     * Boots a fresh kernel and returns its (reboot-disabled) HTTP client, registered for
     * BrowserKit assertions via getClient(). Use before setupFake*Token() to guarantee a clean
     * container in which the auth services are not yet initialized and can therefore be replaced.
     *
     * Because it reboots the kernel (discarding the previous container), it must be called before
     * ANY getContainer()->set() the test relies on - including app-specific service mocks, not just
     * the setupFake*Token() helpers. Mocks registered before bootCleanClient() are thrown away by the
     * reboot and will not be seen by the request. Recommended order: seed the database, then
     * bootCleanClient(), then register every service mock, then setupFake*Token(), then request.
     */
    protected static function bootCleanClient(): KernelBrowser
    {
        self::bootKernel();

        $client = self::getContainer()->get('test.client');
        assert($client instanceof KernelBrowser);

        self::getClient($client);
        $client->disableReboot();

        return $client;
    }

    /**
     * @param list<string> $features
     * @return array<string, mixed>
     */
    private function buildFakeStorageTokenData(string $projectId, array $features, ?string $adminId): array
    {
        $tokenData = [
            'id' => 123,
            'description' => 'foo token',
            'owner' => [
                'id' => $projectId,
                'features' => $features,
            ],
        ];
        if ($adminId !== null) {
            $tokenData['admin'] = ['id' => $adminId];
        }

        return $tokenData;
    }

    /**
     * @param list<string> $features
     */
    private function setupFakeStorageApiToken(
        ?string $tokenString = null,
        string $projectId = '123',
        array $features = [],
        ?string $adminId = null,
    ): StorageApiToken {
        $tokenString ??= uniqid('fakeStorageToken-', true);
        $tokenData = $this->buildFakeStorageTokenData($projectId, $features, $adminId);

        $storageApiClient = $this->createMock(StorageApiClient::class);
        $storageApiClient->method('getTokenString')->willReturn($tokenString);
        $storageApiClient->method('verifyToken')->willReturn($tokenData);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getBasicClient')->willReturn($storageApiClient);

        $clientRequestFactory = $this->createMock(StorageClientRequestFactory::class);
        $clientRequestFactory->method('createClientWrapper')->willReturn($clientWrapper);

        self::getContainer()->set(StorageClientRequestFactory::class, $clientRequestFactory);

        return new StorageApiToken($tokenData, $tokenString);
    }

    /**
     * Stubs the programmatic-token exchange used by transparent #[StorageApiTokenAuth], so guarded
     * controllers can be exercised without reaching Connection or Storage API. The fake resolver
     * client returns a fixed legacy Storage token together with its full token detail, mirroring
     * the real resolver response - no Storage API stub is needed because the exchange no longer
     * verifies the resolved token.
     *
     * @param list<string> $features
     */
    private function setupFakeConnectionToken(
        string $projectId = '123',
        array $features = [],
        ?string $tokenString = null,
        ?string $adminId = null,
    ): StorageApiToken {
        $tokenString ??= uniqid('fakeStorageToken-', true);
        $tokenData = $this->buildFakeStorageTokenData($projectId, $features, $adminId);

        $resolverClient = $this->createMock(ManageApiClient::class);
        $resolverClient
            ->method('resolveStorageToken')
            ->willReturn([
                'storageToken' => $tokenString,
                'projectId' => (int) $projectId,
                'tokenId' => '123',
                'userId' => $adminId ?? '456',
                'expiresAt' => null,
                'tokenDetail' => $tokenData,
            ]);

        self::getContainer()->set(KeboolaApiExtension::STORAGE_TOKEN_RESOLVER_CLIENT_ID, $resolverClient);

        return new StorageApiToken($tokenData, $tokenString);
    }

    /**
     * Stubs the Manage API token verification used by #[ApplicationTokenAuth]. Works
     * whether the request carries the X-KBC-ManageApiToken header or the Kubernetes
     * ServiceAccount JWT (X-Kubernetes-Authorization) — both verify paths return the same token.
     *
     * @param list<string> $scopes
     * @param list<string> $features
     */
    private function setupFakeManageApiToken(string $tokenString, array $scopes = [], array $features = []): void
    {
        $manageApiClient = $this->createMock(ManageApiClient::class);
        $manageApiClient
            ->method('verifyToken')
            ->willReturn([
                'id' => 123,
                'description' => 'foo token',
                'scopes' => $scopes,
                'user' => [
                    'features' => $features,
                ],
            ])
        ;

        $manageApiClientFactory = $this->createMock(ManageApiClientFactory::class);
        $manageApiClientFactory->method('getClientForManageToken')->with($tokenString)->willReturn($manageApiClient);
        $manageApiClientFactory->method('getClientForServiceAccountToken')
            ->with($tokenString)
            ->willReturn($manageApiClient);

        self::getContainer()->set(ManageApiClientFactory::class, $manageApiClientFactory);
    }
}
