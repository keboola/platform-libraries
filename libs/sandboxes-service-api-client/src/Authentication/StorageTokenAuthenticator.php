<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Authentication;

use Psr\Http\Message\RequestInterface;

class StorageTokenAuthenticator
{
    public const STORAGE_TOKEN_HEADER = 'X-StorageApi-Token';
    public function __construct(
        private readonly string $value,
    ) {
    }

    public function __invoke(RequestInterface $request): RequestInterface
    {
        return $request->withHeader(self::STORAGE_TOKEN_HEADER, $this->value);
    }
}
