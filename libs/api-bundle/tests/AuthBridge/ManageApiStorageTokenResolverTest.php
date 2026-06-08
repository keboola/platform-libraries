<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\AuthBridge;

use Generator;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Psr7\Request;
use Keboola\ApiBundle\AuthBridge\Exception\InvalidResolverRequestException;
use Keboola\ApiBundle\AuthBridge\Exception\ProjectAccessDeniedException;
use Keboola\ApiBundle\AuthBridge\Exception\ResolverUnavailableException;
use Keboola\ApiBundle\AuthBridge\Exception\StorageTokenResolverException;
use Keboola\ApiBundle\AuthBridge\Exception\UnauthorizedSubjectTokenException;
use Keboola\ApiBundle\AuthBridge\ManageApiStorageTokenResolver;
use Keboola\ApiBundle\AuthBridge\ResolvedStorageToken;
use Keboola\ApiBundle\Security\ApplicationToken\ManageApiClientFactory;
use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\ManageApi\ClientException;
use Keboola\ManageApi\MaintenanceException;
use Keboola\ServiceClient\ServiceDnsType;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use RuntimeException;

#[CoversClass(ManageApiStorageTokenResolver::class)]
#[CoversClass(ResolvedStorageToken::class)]
class ManageApiStorageTokenResolverTest extends TestCase
{
    private const SUBJECT_TOKEN = 'kbc_at_secret';
    private const TOKEN_PATH = '/var/run/secrets/connection.keboola.com/serviceaccount/token';

    private ManageApiClient&MockObject $manageApiClient;

    protected function setUp(): void
    {
        parent::setUp();
        $this->manageApiClient = $this->createMock(ManageApiClient::class);
    }

    public function testResolveReturnsTokenFromManageApiResponse(): void
    {
        $this->manageApiClient->expects(self::once())
            ->method('resolveStorageToken')
            ->with(123, self::SUBJECT_TOKEN)
            ->willReturn([
                'storageToken' => 'legacy-storage-token',
                'projectId' => 123,
                'tokenId' => '42',
                'userId' => '7',
                'expiresAt' => '2026-05-18T12:30:00+00:00',
            ]);

        $result = $this->createResolver()->resolve(123, self::SUBJECT_TOKEN);

        self::assertSame('legacy-storage-token', $result->storageToken);
        self::assertSame(123, $result->projectId);
        self::assertSame('42', $result->tokenId);
        self::assertSame('7', $result->userId);
        self::assertSame('2026-05-18T12:30:00+00:00', $result->expiresAt);
    }

    public function testResolveBuildsClientWithConfiguredTokenPathAndDnsType(): void
    {
        $this->manageApiClient->expects(self::once())
            ->method('resolveStorageToken')
            ->willReturn([
                'storageToken' => 'legacy-storage-token',
                'projectId' => 123,
                'tokenId' => '42',
                'userId' => '7',
                'expiresAt' => null,
            ]);

        $clientFactory = $this->createMock(ManageApiClientFactory::class);
        $clientFactory->expects(self::once())
            ->method('getClientForServiceAccountTokenPath')
            ->with(self::TOKEN_PATH, ServiceDnsType::INTERNAL)
            ->willReturn($this->manageApiClient);

        $resolver = new ManageApiStorageTokenResolver($clientFactory, self::TOKEN_PATH, ServiceDnsType::INTERNAL);

        $resolver->resolve(123, self::SUBJECT_TOKEN);
    }

    /**
     * @param class-string<StorageTokenResolverException> $expectedException
     */
    #[DataProvider('provideErrorStatusCodes')]
    public function testResolveMapsClientExceptionStatusToException(int $statusCode, string $expectedException): void
    {
        // The Manage API client embeds the response body in the exception message; assert our
        // wrapper never echoes it back so subject/storage token material cannot leak.
        $this->manageApiClient->expects(self::once())
            ->method('resolveStorageToken')
            ->willThrowException(new ClientException(self::SUBJECT_TOKEN, $statusCode));

        try {
            $this->createResolver()->resolve(123, self::SUBJECT_TOKEN);
            self::fail('Expected exception was not thrown.');
        } catch (StorageTokenResolverException $e) {
            self::assertInstanceOf($expectedException, $e);
            self::assertStringNotContainsString(self::SUBJECT_TOKEN, $e->getMessage());
        }
    }

    public static function provideErrorStatusCodes(): Generator
    {
        yield 'bad request' => [
            'statusCode' => 400,
            'expectedException' => InvalidResolverRequestException::class,
        ];
        yield 'unauthorized' => [
            'statusCode' => 401,
            'expectedException' => UnauthorizedSubjectTokenException::class,
        ];
        yield 'forbidden' => [
            'statusCode' => 403,
            'expectedException' => ProjectAccessDeniedException::class,
        ];
        yield 'server error' => [
            'statusCode' => 500,
            'expectedException' => ResolverUnavailableException::class,
        ];
        yield 'connect error without response (code 0)' => [
            'statusCode' => 0,
            'expectedException' => ResolverUnavailableException::class,
        ];
        yield 'unexpected client error' => [
            'statusCode' => 418,
            'expectedException' => StorageTokenResolverException::class,
        ];
    }

    public function testResolveMapsMaintenanceExceptionToUnavailable(): void
    {
        $this->manageApiClient->expects(self::once())
            ->method('resolveStorageToken')
            ->willThrowException(new MaintenanceException('Maintenance', 30, []));

        self::expectException(ResolverUnavailableException::class);

        $this->createResolver()->resolve(123, self::SUBJECT_TOKEN);
    }

    public function testResolveMapsServiceAccountTokenFailureToUnavailable(): void
    {
        // KubernetesServiceAccountTokenAuthenticationStrategy throws RuntimeException when the
        // projected SA token file is missing/unreadable/empty.
        $this->manageApiClient->expects(self::once())
            ->method('resolveStorageToken')
            ->willThrowException(new RuntimeException('token file is empty'));

        self::expectException(ResolverUnavailableException::class);

        $this->createResolver()->resolve(123, self::SUBJECT_TOKEN);
    }

    public function testResolveMapsConnectExceptionToUnavailable(): void
    {
        $this->manageApiClient->expects(self::once())
            ->method('resolveStorageToken')
            ->willThrowException(new ConnectException('Connection refused', new Request('POST', 'resolve')));

        self::expectException(ResolverUnavailableException::class);

        $this->createResolver()->resolve(123, self::SUBJECT_TOKEN);
    }

    public function testResolveThrowsWhenResponseShapeIsInvalid(): void
    {
        $this->manageApiClient->expects(self::once())
            ->method('resolveStorageToken')
            ->willReturn(['projectId' => 123]);

        self::expectException(StorageTokenResolverException::class);

        $this->createResolver()->resolve(123, self::SUBJECT_TOKEN);
    }

    private function createResolver(): ManageApiStorageTokenResolver
    {
        $clientFactory = $this->createMock(ManageApiClientFactory::class);
        $clientFactory->expects(self::once())
            ->method('getClientForServiceAccountTokenPath')
            ->with(self::TOKEN_PATH, ServiceDnsType::INTERNAL)
            ->willReturn($this->manageApiClient);

        return new ManageApiStorageTokenResolver($clientFactory, self::TOKEN_PATH, ServiceDnsType::INTERNAL);
    }
}
