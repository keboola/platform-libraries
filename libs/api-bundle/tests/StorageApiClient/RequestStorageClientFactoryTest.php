<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\StorageApiClient;

use Keboola\ApiBundle\StorageApiClient\RequestStorageClientFactory;
use Keboola\StorageApiBranch\Factory\AuthType;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;

class RequestStorageClientFactoryTest extends TestCase
{
    public function testBuildsWrapperBoundToGivenTokenAndAuthType(): void
    {
        $factory = new RequestStorageClientFactory(new ClientOptions('https://connection.test'));

        $options = $factory
            ->createClientWrapper('my-token', AuthType::BEARER, new Request())
            ->getClientOptionsReadOnly();

        self::assertSame('my-token', $options->getToken());
        self::assertSame(AuthType::BEARER, $options->getAuthType());
        self::assertSame('https://connection.test', $options->getUrl());
    }

    public function testRunIdTakenFromRequestHeader(): void
    {
        $factory = new RequestStorageClientFactory(new ClientOptions('https://connection.test'));

        $wrapper = $factory->createClientWrapper(
            'my-token',
            AuthType::STORAGE_TOKEN,
            new Request([], [], [], [], [], ['HTTP_X_KBC_RUNID' => '42']),
        );

        self::assertSame('42', $wrapper->getClientOptionsReadOnly()->getRunId());
    }

    public function testBaseOptionsAreNotMutatedBetweenCalls(): void
    {
        $base = new ClientOptions('https://connection.test');
        $factory = new RequestStorageClientFactory($base);

        $factory->createClientWrapper('token-a', AuthType::STORAGE_TOKEN, new Request());
        $second = $factory
            ->createClientWrapper('token-b', AuthType::BEARER, new Request())
            ->getClientOptionsReadOnly();

        self::assertNull($base->getToken());
        self::assertSame('token-b', $second->getToken());
        self::assertSame(AuthType::BEARER, $second->getAuthType());
    }
}
