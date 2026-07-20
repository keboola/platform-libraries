<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\StorageApiClient;

use Keboola\ApiBundle\StorageApiClient\StorageClientWrapperFactory;
use Keboola\StorageApiBranch\Factory\AuthType;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;

class StorageClientWrapperFactoryTest extends TestCase
{
    public function testBindsTokenAndStorageAuthType(): void
    {
        $options = StorageClientWrapperFactory::create(
            new ClientOptions('https://connection.test'),
            'my-token',
            AuthType::STORAGE_TOKEN,
            new Request(),
        )->getClientOptionsReadOnly();

        self::assertSame('my-token', $options->getToken());
        self::assertSame(AuthType::STORAGE_TOKEN, $options->getAuthType());
    }

    public function testBindsBearerAuthType(): void
    {
        $options = StorageClientWrapperFactory::create(
            new ClientOptions('https://connection.test'),
            'bearer-token',
            AuthType::BEARER,
            new Request(),
        )->getClientOptionsReadOnly();

        self::assertSame('bearer-token', $options->getToken());
        self::assertSame(AuthType::BEARER, $options->getAuthType());
    }

    public function testRunIdTakenFromRequestHeaderWhenPresent(): void
    {
        $wrapper = StorageClientWrapperFactory::create(
            new ClientOptions('https://connection.test'),
            'my-token',
            AuthType::STORAGE_TOKEN,
            new Request([], [], [], [], [], ['HTTP_X_KBC_RUNID' => '42']),
        );

        self::assertSame('42', $wrapper->getClientOptionsReadOnly()->getRunId());
    }

    public function testRunIdFallsBackToGeneratedValueWhenHeaderMissing(): void
    {
        $wrapper = StorageClientWrapperFactory::create(
            new ClientOptions('https://connection.test'),
            'my-token',
            AuthType::STORAGE_TOKEN,
            new Request(),
        );

        self::assertStringStartsWith('run-', (string) $wrapper->getClientOptionsReadOnly()->getRunId());
    }

    public function testRunIdGeneratorUsedWhenHeaderMissing(): void
    {
        $base = new ClientOptions('https://connection.test');
        $base->setRunIdGenerator(fn (ClientOptions $o): string => 'gen-' . $o->getUrl());

        $wrapper = StorageClientWrapperFactory::create($base, 'my-token', AuthType::STORAGE_TOKEN, new Request());

        self::assertSame('gen-https://connection.test', $wrapper->getClientOptionsReadOnly()->getRunId());
    }

    public function testPerCallOverridesAreMergedButResolvedTokenAndAuthTypeWin(): void
    {
        $options = StorageClientWrapperFactory::create(
            new ClientOptions('https://connection.test'),
            'my-token',
            AuthType::STORAGE_TOKEN,
            new Request(),
            new ClientOptions(token: 'override-token', branchId: '777', authType: AuthType::BEARER),
        )->getClientOptionsReadOnly();

        self::assertSame('777', $options->getBranchId());
        // token/authType are pinned after the overrides are merged, so the resolved values win
        self::assertSame('my-token', $options->getToken());
        self::assertSame(AuthType::STORAGE_TOKEN, $options->getAuthType());
    }

    public function testBaseOptionsAreNotMutated(): void
    {
        $base = new ClientOptions('https://connection.test');

        StorageClientWrapperFactory::create($base, 'my-token', AuthType::STORAGE_TOKEN, new Request());

        self::assertNull($base->getToken());
        self::assertNull($base->getAuthType());
    }
}
