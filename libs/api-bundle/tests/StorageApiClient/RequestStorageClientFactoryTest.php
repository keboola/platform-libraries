<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\StorageApiClient;

use Keboola\ApiBundle\StorageApiClient\RequestStorageClientFactory;
use Keboola\StorageApiBranch\Factory\AuthType;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\StorageApiToken;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;

class RequestStorageClientFactoryTest extends TestCase
{
    private static function factory(ClientOptions $baseClientOptions, Request $request): RequestStorageClientFactory
    {
        return new RequestStorageClientFactory($baseClientOptions, $request, new StorageApiToken([], 'bound-token'));
    }

    public function testCreateClientWrapperUsesBoundTokenWithStorageTokenAuth(): void
    {
        $factory = self::factory(new ClientOptions('https://connection.test'), new Request());

        $options = $factory->createClientWrapper()->getClientOptionsReadOnly();

        self::assertSame('bound-token', $options->getToken());
        self::assertSame(AuthType::STORAGE_TOKEN, $options->getAuthType());
    }

    public function testRunIdTakenFromRequestHeaderWhenPresent(): void
    {
        $request = new Request([], [], [], [], [], ['HTTP_X_KBC_RUNID' => '42']);
        $factory = self::factory(new ClientOptions('https://connection.test'), $request);

        self::assertSame('42', $factory->createClientWrapper()->getClientOptionsReadOnly()->getRunId());
    }

    public function testRunIdFallsBackToGeneratedValueWhenHeaderMissing(): void
    {
        $factory = self::factory(new ClientOptions('https://connection.test'), new Request());

        self::assertStringStartsWith(
            'run-',
            (string) $factory->createClientWrapper()->getClientOptionsReadOnly()->getRunId(),
        );
    }

    public function testRunIdGeneratorUsedWhenHeaderMissing(): void
    {
        $baseOptions = new ClientOptions('https://connection.test');
        $baseOptions->setRunIdGenerator(fn (ClientOptions $o): string => 'gen-' . $o->getUrl());
        $factory = self::factory($baseOptions, new Request());

        self::assertSame(
            'gen-https://connection.test',
            $factory->createClientWrapper()->getClientOptionsReadOnly()->getRunId(),
        );
    }

    public function testPerCallClientOptionsAreMergedOverBase(): void
    {
        $factory = self::factory(new ClientOptions('https://connection.test'), new Request());

        $options = $factory->createClientWrapper(new ClientOptions(branchId: '777'))->getClientOptionsReadOnly();

        self::assertSame('777', $options->getBranchId());
        self::assertSame('https://connection.test', $options->getUrl());
        self::assertSame('bound-token', $options->getToken());
    }

    public function testBaseOptionsAreNotMutated(): void
    {
        $baseOptions = new ClientOptions('https://connection.test');
        $factory = self::factory($baseOptions, new Request());

        $factory->createClientWrapper();

        self::assertNull($baseOptions->getToken());
        self::assertNull($baseOptions->getAuthType());
    }
}
