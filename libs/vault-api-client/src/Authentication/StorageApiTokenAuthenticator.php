<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient\Authentication;

use Psr\Http\Message\RequestInterface;

class StorageApiTokenAuthenticator
{
    public function __construct(
        private readonly string $value,
    ) {
    }

    public function __invoke(RequestInterface $request): RequestInterface
    {
        return $request->withHeader('X-StorageApi-Token', $this->value);
    }
}
