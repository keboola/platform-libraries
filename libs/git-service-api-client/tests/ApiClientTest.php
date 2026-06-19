<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\ApiClientBase\ApiClient;
use Keboola\ApiClientBase\ApiClientOptions;
use Keboola\ApiClientBase\Auth\KeboolaServiceAccountAuthenticator;
use Keboola\GitServiceApiClient\Exception\GitServiceClientException;
use Keboola\GitServiceApiClient\GitServiceApiClient;
use Keboola\GitServiceApiClient\Model\Repository;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * Tests that exercise transport-level behaviour of GitServiceApiClient
 * (auth headers, retry, error mapping). The underlying transport is now
 * provided by keboola/php-api-client-base; these tests confirm the facade
 * wires it up correctly for git-service's specific auth and error-format.
 */
class ApiClientTest extends TestCase
{
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
        $mock = new MockHandler([new Response(200, [], (string) json_encode([
            'name' => 'app-1',
            'createdAt' => 't',
            'defaultBranch' => 'main',
            'sshUrl' => 's',
            'httpsUrl' => 'h',
        ]))]);
        $stack = HandlerStack::create($mock);

        $client = new GitServiceApiClient(
            'https://example.test',
            manageToken: 'secret-token',
            requestHandler: $stack,
        );
        $client->getRepository('app-1');

        $lastRequest = $mock->getLastRequest();
        self::assertNotNull($lastRequest);
        self::assertSame('secret-token', $lastRequest->getHeader('X-KBC-ManageApiToken')[0]);
        self::assertSame([], $lastRequest->getHeader('X-Kubernetes-Authorization'));
    }

    public function testKeboolaSaAuthRereadsTokenEachRequest(): void
    {
        $tokenPath = (string) tempnam(sys_get_temp_dir(), 'kbla-sa-token-');
        self::assertNotSame('', $tokenPath);
        try {
            file_put_contents($tokenPath, "first-token\n");

            $mock = new MockHandler([
                new Response(204),
                new Response(204),
            ]);
            $stack = HandlerStack::create($mock);

            // Use the base authenticator directly, which accepts a custom path.
            // The facade always uses the default path for SA auth, so we test
            // the re-read behaviour at the authenticator level here.
            $auth = new KeboolaServiceAccountAuthenticator($tokenPath);
            $apiClient = new ApiClient(
                'https://example.test',
                $auth,
                new ApiClientOptions(requestHandler: $stack),
            );

            $apiClient->sendRequest(new Request('DELETE', 'repos/app-1'));
            $lastRequest = $mock->getLastRequest();
            self::assertNotNull($lastRequest);
            self::assertSame(
                'Bearer first-token',
                $lastRequest->getHeader('X-Kubernetes-Authorization')[0],
            );

            // Simulate kubelet rotating the projected token file.
            file_put_contents($tokenPath, "second-token\n");

            $apiClient->sendRequest(new Request('DELETE', 'repos/app-2'));
            $lastRequest = $mock->getLastRequest();
            self::assertNotNull($lastRequest);
            self::assertSame(
                'Bearer second-token',
                $lastRequest->getHeader('X-Kubernetes-Authorization')[0],
            );
        } finally {
            @unlink($tokenPath);
        }
    }

    public function testRetriesOn5xx(): void
    {
        $mock = new MockHandler([
            new Response(500),
            new Response(500),
            new Response(204),
        ]);
        $stack = HandlerStack::create($mock);

        $client = new GitServiceApiClient(
            'https://example.test',
            manageToken: 'token',
            backoffMaxTries: 3,
            requestHandler: $stack,
        );
        $client->deleteRepository('app-1');

        // If retries didn't fire, MockHandler would still hold remaining responses.
        self::assertSame(0, $mock->count());
    }

    public function testThrowsClientExceptionOn4xxWithErrorCodeBody(): void
    {
        $mock = new MockHandler([
            new Response(404, [], '{"code":"repository.notFound","error":"repo missing"}'),
        ]);
        $stack = HandlerStack::create($mock);

        $client = new GitServiceApiClient(
            'https://example.test',
            manageToken: 'token',
            requestHandler: $stack,
        );

        $this->expectException(GitServiceClientException::class);
        $this->expectExceptionMessage('repository.notFound: repo missing');
        $this->expectExceptionCode(404);
        $client->deleteRepository('app-1');
    }

    public function testThrowsClientExceptionOn4xxWithoutJson(): void
    {
        $mock = new MockHandler([new Response(400, [], 'plain text error')]);
        $stack = HandlerStack::create($mock);

        $client = new GitServiceApiClient(
            'https://example.test',
            manageToken: 'token',
            requestHandler: $stack,
        );

        $this->expectException(GitServiceClientException::class);
        $this->expectExceptionCode(400);
        $client->deleteRepository('app-1');
    }

    public function testMapsResponseIntoSingleModel(): void
    {
        $mock = new MockHandler([new Response(200, [], (string) json_encode([
            'name' => 'app-1',
            'createdAt' => 'now',
            'defaultBranch' => 'main',
            'sshUrl' => 'ssh://git/app-1',
            'httpsUrl' => 'https://git/app-1.git',
        ]))]);
        $stack = HandlerStack::create($mock);

        $client = new GitServiceApiClient(
            'https://example.test',
            manageToken: 'token',
            requestHandler: $stack,
        );

        $repo = $client->getRepository('app-1');

        self::assertSame('app-1', $repo->name);
    }

    public function testThrowsClientExceptionOnInvalidJson(): void
    {
        $mock = new MockHandler([new Response(200, [], 'not json')]);
        $stack = HandlerStack::create($mock);

        $client = new GitServiceApiClient(
            'https://example.test',
            manageToken: 'token',
            requestHandler: $stack,
        );

        $this->expectException(GitServiceClientException::class);
        $client->getRepository('app-1');
    }
}
