<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\AuthBridge;

use Generator;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\ApiBundle\AuthBridge\AuthBridgeStorageTokenResolver;
use Keboola\ApiBundle\AuthBridge\Exception\InvalidResolverRequestException;
use Keboola\ApiBundle\AuthBridge\Exception\ProjectAccessDeniedException;
use Keboola\ApiBundle\AuthBridge\Exception\ResolverUnavailableException;
use Keboola\ApiBundle\AuthBridge\Exception\StorageTokenResolverException;
use Keboola\ApiBundle\AuthBridge\Exception\UnauthorizedSubjectTokenException;
use Keboola\ApiBundle\AuthBridge\KubernetesServiceAccountTokenProvider;
use Keboola\ApiBundle\AuthBridge\ResolvedStorageToken;
use Keboola\ServiceClient\ServiceClient;
use Keboola\ServiceClient\ServiceDnsType;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

#[CoversClass(AuthBridgeStorageTokenResolver::class)]
#[CoversClass(ResolvedStorageToken::class)]
class AuthBridgeStorageTokenResolverTest extends TestCase
{
    private const SUBJECT_TOKEN = 'kbc_at_secret';

    private KubernetesServiceAccountTokenProvider&MockObject $tokenProvider;

    protected function setUp(): void
    {
        parent::setUp();
        $this->tokenProvider = $this->createMock(KubernetesServiceAccountTokenProvider::class);
    }

    public function testResolveReturnsTokenAndSendsExpectedRequest(): void
    {
        $this->tokenProvider->expects(self::once())->method('getToken')->willReturn('sa-jwt');

        $mockHandler = new MockHandler([
            new Response(200, [], (string) json_encode([
                'storageToken' => 'legacy-storage-token',
                'projectId' => 123,
                'tokenId' => '42',
                'userId' => '7',
                'expiresAt' => '2026-05-18T12:30:00+00:00',
            ])),
        ]);

        $result = $this->createResolver($mockHandler)->resolve(123, self::SUBJECT_TOKEN);

        self::assertSame('legacy-storage-token', $result->storageToken);
        self::assertSame(123, $result->projectId);
        self::assertSame('42', $result->tokenId);
        self::assertSame('7', $result->userId);
        self::assertSame('2026-05-18T12:30:00+00:00', $result->expiresAt);

        $request = $mockHandler->getLastRequest();
        self::assertNotNull($request);
        self::assertSame('POST', $request->getMethod());
        self::assertStringEndsWith(
            '/manage/internal/auth-bridge/resolve-storage-token',
            $request->getUri()->getPath(),
        );
        self::assertSame('Bearer sa-jwt', $request->getHeaderLine('X-Kubernetes-Authorization'));
        self::assertSame('Bearer ' . self::SUBJECT_TOKEN, $request->getHeaderLine('X-Subject-Token'));
        self::assertSame(['projectId' => 123], json_decode((string) $request->getBody(), true));
    }

    /**
     * @param class-string<StorageTokenResolverException> $expectedException
     */
    #[DataProvider('provideErrorStatusCodes')]
    public function testResolveMapsErrorStatusToException(int $statusCode, string $expectedException): void
    {
        $this->tokenProvider->expects(self::once())->method('getToken')->willReturn('sa-jwt');

        $mockHandler = new MockHandler([
            new Response($statusCode, [], (string) json_encode(['error' => self::SUBJECT_TOKEN])),
        ]);

        try {
            $this->createResolver($mockHandler)->resolve(123, self::SUBJECT_TOKEN);
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
        yield 'unexpected client error' => [
            'statusCode' => 418,
            'expectedException' => StorageTokenResolverException::class,
        ];
    }

    public function testResolveMapsConnectExceptionToUnavailable(): void
    {
        $this->tokenProvider->expects(self::once())->method('getToken')->willReturn('sa-jwt');

        $mockHandler = new MockHandler([
            new ConnectException('Connection refused', new Request('POST', 'resolve-storage-token')),
        ]);

        self::expectException(ResolverUnavailableException::class);

        $this->createResolver($mockHandler)->resolve(123, self::SUBJECT_TOKEN);
    }

    public function testResolveThrowsOnInvalidJson(): void
    {
        $this->tokenProvider->expects(self::once())->method('getToken')->willReturn('sa-jwt');

        $mockHandler = new MockHandler([
            new Response(200, [], 'this-is-not-json'),
        ]);

        self::expectException(StorageTokenResolverException::class);

        $this->createResolver($mockHandler)->resolve(123, self::SUBJECT_TOKEN);
    }

    public function testResolveThrowsWhenResponseIsNotAnArray(): void
    {
        $this->tokenProvider->expects(self::once())->method('getToken')->willReturn('sa-jwt');

        $mockHandler = new MockHandler([
            new Response(200, [], '"a-plain-string"'),
        ]);

        self::expectException(StorageTokenResolverException::class);

        $this->createResolver($mockHandler)->resolve(123, self::SUBJECT_TOKEN);
    }

    public function testResolvePropagatesServiceAccountTokenFailureWithoutHttpCall(): void
    {
        $this->tokenProvider->expects(self::once())
            ->method('getToken')
            ->willThrowException(new ResolverUnavailableException('Service account token file is empty.'));

        $mockHandler = new MockHandler([]);

        try {
            $this->createResolver($mockHandler)->resolve(123, self::SUBJECT_TOKEN);
            self::fail('Expected exception was not thrown.');
        } catch (ResolverUnavailableException) {
            self::assertNull($mockHandler->getLastRequest(), 'No HTTP request should be sent.');
        }
    }

    private function createResolver(MockHandler $mockHandler): AuthBridgeStorageTokenResolver
    {
        return new AuthBridgeStorageTokenResolver(
            new ServiceClient('keboola.com'),
            $this->tokenProvider,
            ServiceDnsType::INTERNAL,
            null,
            new NullLogger(),
            HandlerStack::create($mockHandler),
        );
    }
}
