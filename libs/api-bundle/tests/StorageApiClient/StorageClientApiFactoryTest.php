<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\StorageApiClient;

use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken as SecurityStorageApiToken;
use Keboola\ApiBundle\StorageApiClient\StorageClientApiFactory;
use Keboola\StorageApiBranch\Factory\AuthType;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\StorageApiToken;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;

class StorageClientApiFactoryTest extends TestCase
{
    private const RUN_ID_HEADER = 'HTTP_X_KBC_RUNID';
    private const TOKEN_HEADER = 'HTTP_X_STORAGEAPI_TOKEN';
    private const AUTHORIZATION_HEADER = 'HTTP_AUTHORIZATION';

    private static function storageApiToken(string $tokenValue): StorageApiToken
    {
        return new StorageApiToken([], $tokenValue);
    }

    public function testUsesTokenFromObjectAndIgnoresRequestHeaders(): void
    {
        // The request carries unrelated/PAT token material in the headers; the factory must
        // ignore it and use the resolved token from the StorageApiToken object instead.
        $request = new Request([], [], [], [], [], [
            self::TOKEN_HEADER => 'header-storage-token',
            self::AUTHORIZATION_HEADER => 'Bearer kbc_pat_from_header',
        ]);
        $factory = new StorageClientApiFactory(new ClientOptions('https://connection.test'));

        $wrapper = $factory->createClientWrapper($request, self::storageApiToken('resolved-storage-token'));

        self::assertSame('resolved-storage-token', $wrapper->getClientOptionsReadOnly()->getToken());
        self::assertSame(AuthType::STORAGE_TOKEN, $wrapper->getClientOptionsReadOnly()->getAuthType());
    }

    public function testRunIdTakenFromHeaderWhenPresent(): void
    {
        $request = new Request([], [], [], [], [], [self::RUN_ID_HEADER => '123']);
        $factory = new StorageClientApiFactory(new ClientOptions('https://connection.test'));

        $wrapper = $factory->createClientWrapper($request, self::storageApiToken('t'));

        self::assertSame('123', $wrapper->getClientOptionsReadOnly()->getRunId());
    }

    public function testRunIdFallsBackToGeneratedValueWhenHeaderMissing(): void
    {
        $request = new Request();
        $factory = new StorageClientApiFactory(new ClientOptions('https://connection.test'));

        $wrapper = $factory->createClientWrapper($request, self::storageApiToken('t'));

        self::assertStringStartsWith('run-', (string) $wrapper->getClientOptionsReadOnly()->getRunId());
    }

    public function testRunIdGeneratorUsedWhenHeaderMissing(): void
    {
        $request = new Request();
        $options = new ClientOptions('https://connection.test');
        $options->setRunIdGenerator(fn (ClientOptions $o): string => 'gen-' . $o->getUrl());
        $factory = new StorageClientApiFactory($options);

        $wrapper = $factory->createClientWrapper($request, self::storageApiToken('t'));

        self::assertSame('gen-https://connection.test', $wrapper->getClientOptionsReadOnly()->getRunId());
    }

    public function testPerCallClientOptionsAreMergedOverBase(): void
    {
        $request = new Request();
        $factory = new StorageClientApiFactory(new ClientOptions('https://connection.test'));

        $wrapper = $factory->createClientWrapper(
            $request,
            self::storageApiToken('t'),
            new ClientOptions(branchId: '1234'),
        );

        self::assertSame('1234', $wrapper->getClientOptionsReadOnly()->getBranchId());
        self::assertSame('https://connection.test', $wrapper->getClientOptionsReadOnly()->getUrl());
    }

    public function testBaseOptionsAreNotMutated(): void
    {
        $request = new Request();
        $base = new ClientOptions('https://connection.test');
        $factory = new StorageClientApiFactory($base);

        $factory->createClientWrapper($request, self::storageApiToken('t'));

        // The token must not leak back into the caller's base options nor the factory's own copy.
        self::assertNull($base->getToken());
        self::assertNull($factory->getClientOptionsReadOnly()->getToken());
    }

    public function testGetClientOptionsReadOnlyReturnsIsolatedClone(): void
    {
        $factory = new StorageClientApiFactory(new ClientOptions('https://foo'));

        self::assertSame('https://foo', $factory->getClientOptionsReadOnly()->getUrl());
        $factory->getClientOptionsReadOnly()->setUrl('https://bar');
        self::assertSame('https://foo', $factory->getClientOptionsReadOnly()->getUrl());
    }

    public function testAcceptsSecurityStorageApiTokenSubclass(): void
    {
        $request = new Request();
        $factory = new StorageClientApiFactory(new ClientOptions('https://connection.test'));
        $token = new SecurityStorageApiToken([], 'subclass-token');

        $wrapper = $factory->createClientWrapper($request, $token);

        self::assertSame('subclass-token', $wrapper->getClientOptionsReadOnly()->getToken());
    }
}
