<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests;

use Keboola\ApiClientBase\ApiClient;
use Keboola\ApiClientBase\ApiClientOptions;
use Keboola\ApiClientBase\Auth\KeboolaServiceAccountAuthenticator;
use Keboola\ApiClientBase\Exception\ClientException;
use Keboola\GitServiceApiClient\GitServiceApiClient;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\MockResponse;

/**
 * Tests that exercise transport-level behaviour of GitServiceApiClient
 * (auth headers, retry, error mapping). The underlying transport is now
 * provided by keboola/php-api-client-base on Symfony HttpClient; these tests
 * confirm the facade wires it up correctly for git-service's specific auth and
 * error-format.
 */
class ApiClientTest extends TestCase
{
    /**
     * @return array<string, string>
     */
    private function requestHeaders(MockResponse $response): array
    {
        $headers = [];
        /** @var list<string> $lines */
        $lines = $response->getRequestOptions()['headers'] ?? [];
        foreach ($lines as $line) {
            [$name, $value] = explode(': ', $line, 2);
            $headers[strtolower($name)] = $value;
        }
        return $headers;
    }

    public function testDefaultAuthThrowsOnFirstRequestWhenNoSaTokenFileExists(): void
    {
        // Assumes the test container is NOT running with a projected
        // connection-token at the Keboola SA path (true in CI).
        $defaultPath = KeboolaServiceAccountAuthenticator::DEFAULT_TOKEN_PATH;
        if (is_readable($defaultPath)) {
            self::markTestSkipped(sprintf(
                'Keboola SA token at "%s" is mounted in this environment; '
                    . 'cannot exercise the default-auth failure path.',
                $defaultPath,
            ));
        }

        // Construction succeeds (the default auth is lazy); the first outbound
        // request triggers the file read and the resulting failure.
        $client = new GitServiceApiClient('https://example.test');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage($defaultPath);
        // Any domain method that issues a request works; deleteRepository is void and simple.
        $client->deleteRepository('some-repo');
    }

    public function testAddsManageApiTokenAuthHeader(): void
    {
        $response = new MockResponse((string) json_encode([
            'name' => 'app-1',
            'createdAt' => 't',
            'defaultBranch' => 'main',
            'sshUrl' => 's',
            'httpsUrl' => 'h',
        ]));
        $client = new GitServiceApiClient(
            'https://example.test',
            manageToken: 'secret-token',
            httpClient: new MockHttpClient($response, 'https://example.test/'),
        );
        $client->getRepository('app-1');

        $headers = $this->requestHeaders($response);
        self::assertSame('secret-token', $headers['x-kbc-manageapitoken'] ?? null);
        self::assertArrayNotHasKey('x-kubernetes-authorization', $headers);
    }

    public function testKeboolaSaAuthRereadsTokenEachRequest(): void
    {
        $tokenPath = (string) tempnam(sys_get_temp_dir(), 'kbla-sa-token-');
        self::assertNotSame('', $tokenPath);
        try {
            file_put_contents($tokenPath, "first-token\n");

            $first = new MockResponse('', ['http_code' => 204]);
            $second = new MockResponse('', ['http_code' => 204]);
            $mock = new MockHttpClient([$first, $second], 'https://example.test/');

            // Use the base authenticator directly, which accepts a custom path.
            // The facade always uses the default path for SA auth, so we test
            // the re-read behaviour at the authenticator level here.
            $auth = new KeboolaServiceAccountAuthenticator($tokenPath);
            $apiClient = new ApiClient(
                'https://example.test',
                $auth,
                new ApiClientOptions(httpClient: $mock),
            );

            $apiClient->sendRequest('DELETE', 'repos/app-1');
            self::assertSame(
                'Bearer first-token',
                $this->requestHeaders($first)['x-kubernetes-authorization'] ?? null,
            );

            // Simulate kubelet rotating the projected token file.
            file_put_contents($tokenPath, "second-token\n");

            $apiClient->sendRequest('DELETE', 'repos/app-2');
            self::assertSame(
                'Bearer second-token',
                $this->requestHeaders($second)['x-kubernetes-authorization'] ?? null,
            );
        } finally {
            @unlink($tokenPath);
        }
    }

    public function testRetriesOn5xx(): void
    {
        $mock = new MockHttpClient([
            new MockResponse('', ['http_code' => 500]),
            new MockResponse('', ['http_code' => 500]),
            new MockResponse('', ['http_code' => 204]),
        ], 'https://example.test/');

        $client = new GitServiceApiClient(
            'https://example.test',
            manageToken: 'token',
            backoffMaxTries: 3,
            httpClient: $mock,
        );
        $client->deleteRepository('app-1');

        self::assertSame(3, $mock->getRequestsCount());
    }

    public function testThrowsClientExceptionOn4xxWithErrorCodeBody(): void
    {
        $mock = new MockHttpClient([
            new MockResponse('{"code":"repository.notFound","error":"repo missing"}', ['http_code' => 404]),
        ], 'https://example.test/');

        $client = new GitServiceApiClient(
            'https://example.test',
            manageToken: 'token',
            httpClient: $mock,
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('repository.notFound: repo missing');
        $this->expectExceptionCode(404);
        $client->deleteRepository('app-1');
    }

    public function testThrowsClientExceptionOn4xxWithoutJson(): void
    {
        $mock = new MockHttpClient(
            [new MockResponse('plain text error', ['http_code' => 400])],
            'https://example.test/',
        );

        $client = new GitServiceApiClient(
            'https://example.test',
            manageToken: 'token',
            httpClient: $mock,
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionCode(400);
        $client->deleteRepository('app-1');
    }

    public function testMapsResponseIntoSingleModel(): void
    {
        $mock = new MockHttpClient([new MockResponse((string) json_encode([
            'name' => 'app-1',
            'createdAt' => 'now',
            'defaultBranch' => 'main',
            'sshUrl' => 'ssh://git/app-1',
            'httpsUrl' => 'https://git/app-1.git',
        ]))], 'https://example.test/');

        $client = new GitServiceApiClient(
            'https://example.test',
            manageToken: 'token',
            httpClient: $mock,
        );

        $repo = $client->getRepository('app-1');

        self::assertSame('app-1', $repo->name);
    }

    public function testThrowsClientExceptionOnInvalidJson(): void
    {
        $mock = new MockHttpClient([new MockResponse('not json')], 'https://example.test/');

        $client = new GitServiceApiClient(
            'https://example.test',
            manageToken: 'token',
            httpClient: $mock,
        );

        $this->expectException(ClientException::class);
        $client->getRepository('app-1');
    }
}
