<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\StorageApiClient;

use Keboola\ApiBundle\StorageApiClient\RequestStorageClientFactory;
use Keboola\ApiBundle\StorageApiClient\StorageClientApiFactory;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\StorageApiToken;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;

class RequestStorageClientFactoryTest extends TestCase
{
    public function testCreateClientWrapperUsesBoundRequestAndToken(): void
    {
        $request = new Request([], [], [], [], [], ['HTTP_X_KBC_RUNID' => '42']);
        $token = new StorageApiToken([], 'bound-token');
        $base = new StorageClientApiFactory(new ClientOptions('https://connection.test'));

        $factory = new RequestStorageClientFactory($base, $request, $token);
        $wrapper = $factory->createClientWrapper();

        self::assertSame('bound-token', $wrapper->getClientOptionsReadOnly()->getToken());
        self::assertSame('42', $wrapper->getClientOptionsReadOnly()->getRunId());
    }

    public function testCreateClientWrapperMergesPerCallOptions(): void
    {
        $request = new Request();
        $token = new StorageApiToken([], 'bound-token');
        $base = new StorageClientApiFactory(new ClientOptions('https://connection.test'));

        $factory = new RequestStorageClientFactory($base, $request, $token);
        $wrapper = $factory->createClientWrapper(new ClientOptions(branchId: '777'));

        self::assertSame('777', $wrapper->getClientOptionsReadOnly()->getBranchId());
        self::assertSame('bound-token', $wrapper->getClientOptionsReadOnly()->getToken());
    }
}
