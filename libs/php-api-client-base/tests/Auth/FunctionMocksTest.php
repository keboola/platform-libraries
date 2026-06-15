<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Auth;

use GuzzleHttp\Psr7\Request;
use Keboola\ApiClientBase\Auth\KeboolaServiceAccountAuthenticator;
use PHPUnit\Framework\TestCase;

class FunctionMocksTest extends TestCase
{
    protected function tearDown(): void
    {
        FunctionMocks::reset();
        parent::tearDown();
    }

    public function testShadowedFileGetContentsDrivesTheAuthenticator(): void
    {
        FunctionMocks::enable(['scripted-token']);

        $authenticator = new KeboolaServiceAccountAuthenticator('/fake/sa/token');
        $request = $authenticator(new Request('GET', 'https://example.test'));

        self::assertSame('Bearer scripted-token', $request->getHeaderLine('X-Kubernetes-Authorization'));
        self::assertSame(1, FunctionMocks::readCount());
    }

    public function testUsleepShadowRecordsWithoutSleeping(): void
    {
        FunctionMocks::enable([]);
        // phpcs:ignore SlevomatCodingStandard.Namespaces.ReferenceUsedNamesOnly.ReferenceViaFullyQualifiedName
        \Keboola\ApiClientBase\Auth\usleep(12345);

        self::assertSame([12345], FunctionMocks::recordedSleeps());
    }
}
