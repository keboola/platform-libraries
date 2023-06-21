<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient;

use Psr\Http\Message\RequestInterface;

class ApiClient
{
    public function sendRequestAndMapResponse(
        RequestInterface $request,
        string $responseClass,
        array $options = [],
        bool $isList = false,
    ): void {
    }

    public function sendRequest(RequestInterface $request): void
    {
    }
}
