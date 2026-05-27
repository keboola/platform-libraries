<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Test;

use Keboola\ApiBundle\Security\KubernetesServiceAccount\ManageApiClientFactory;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken;
use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\StorageClientRequestFactory;
use PHPUnit\Framework\MockObject\MockObject;
use Symfony\Component\DependencyInjection\ContainerInterface;

/**
 * Helpers for functional (KernelTestCase) tests that stub api-bundle's authenticators, so
 * controllers guarded by #[StorageApiTokenAuth] / #[KubernetesServiceAccountAuth] can be
 * exercised without reaching real Storage/Manage APIs. The consuming test case provides
 * createMock() and getContainer() (e.g. via KernelTestCase + MockObject).
 */
trait AuthenticatorTestTrait
{
    /**
     * @param class-string $className
     */
    abstract protected function createMock(string $className): MockObject;

    abstract protected static function getContainer(): ContainerInterface;

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
     * Stubs the Manage API token verification used by #[KubernetesServiceAccountAuth]. Works
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
