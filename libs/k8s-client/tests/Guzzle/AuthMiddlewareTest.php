<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\Guzzle;

use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\K8sClient\ClientFacadeFactory\Token\StaticToken;
use Keboola\K8sClient\Guzzle\AuthMiddleware;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

class AuthMiddlewareTest extends TestCase
{
    public function testAuthMiddleware(): void
    {
        $token = new StaticToken('foo-token');
        $middleware = new AuthMiddleware($token);

        $request = new Request('GET', 'https://example.com', ['X-Foo' => 'Bar']);
        $options = ['foo' => 'bar'];
        $response = new Response();

        $handler = function (RequestInterface $passedRequest, array $passedOptions) use ($options, $response) {
            self::assertSame([
                'Host' => ['example.com'],
                'X-Foo' => ['Bar'],
                'Authorization' => ['Bearer foo-token'],
            ], $passedRequest->getHeaders());

            self::assertSame($options, $passedOptions);
            return $response;
        };

        $result = $middleware($handler)($request, $options);
        self::assertSame($response, $result);
    }
}
