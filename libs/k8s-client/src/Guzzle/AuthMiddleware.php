<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Guzzle;

use Keboola\K8sClient\ClientFacadeFactory\Token\TokenInterface;
use Psr\Http\Message\RequestInterface;

readonly class AuthMiddleware
{
    public function __construct(
        private TokenInterface $token,
    ) {
    }

    public function __invoke(callable $handler): callable
    {
        return function (RequestInterface $request, array $options) use ($handler) {
            $request = $request->withHeader('Authorization', 'Bearer ' . $this->token->getValue());
            return $handler($request, $options);
        };
    }
}
