<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\ApiClientFactory;

use Psr\Http\Message\RequestInterface;

interface AuthorizationHeaderResolverInterface
{
    public function __invoke(RequestInterface $request): RequestInterface;
}
