<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Auth;

use Keboola\ApiClientBase\Auth\AuthenticatingHttpClient;
use Keboola\ApiClientBase\Auth\ManageApiTokenAuthenticator;
use Keboola\ApiClientBase\Auth\RequestAuthenticatorInterface;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\MockResponse;

class AuthenticatingHttpClientTest extends TestCase
{
    /**
     * @return array<string, string>
     */
    private function recordedHeaders(MockResponse $response): array
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

    public function testMergesAuthenticatorHeadersIntoRequest(): void
    {
        $response = new MockResponse('{}');
        $client = new AuthenticatingHttpClient(
            new ManageApiTokenAuthenticator('secret'),
            new MockHttpClient($response, 'https://example.test/'),
        );

        $client->request('GET', 'foo')->getStatusCode();

        self::assertSame('secret', $this->recordedHeaders($response)['x-kbc-manageapitoken'] ?? null);
    }

    public function testResolvesAuthenticatorOnEachRequest(): void
    {
        $authenticator = new class implements RequestAuthenticatorInterface {
            public int $calls = 0;

            public function getAuthenticationHeaders(): array
            {
                $this->calls++;
                return ['X-Attempt' => (string) $this->calls];
            }
        };
        $client = new AuthenticatingHttpClient(
            $authenticator,
            new MockHttpClient([new MockResponse('{}'), new MockResponse('{}')], 'https://example.test/'),
        );

        $client->request('GET', 'a')->getStatusCode();
        $client->request('GET', 'b')->getStatusCode();

        self::assertSame(2, $authenticator->calls);
    }

    public function testPerRequestHeaderOverridesAuthHeader(): void
    {
        $response = new MockResponse('{}');
        $client = new AuthenticatingHttpClient(
            new ManageApiTokenAuthenticator('auth-value'),
            new MockHttpClient($response, 'https://example.test/'),
        );

        $client->request('GET', 'foo', ['headers' => ['X-KBC-ManageApiToken' => 'override']])
            ->getStatusCode();

        self::assertSame('override', $this->recordedHeaders($response)['x-kbc-manageapitoken'] ?? null);
    }
}
