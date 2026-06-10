<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase;

use Psr\Log\LoggerInterface;
use Symfony\Contracts\HttpClient\HttpClientInterface;

class ApiClientOptions
{
    public const DEFAULT_BACKOFF_MAX_TRIES = 5;
    public const DEFAULT_CONNECT_TIMEOUT = 10;
    public const DEFAULT_REQUEST_TIMEOUT = 120;

    /**
     * @param int<0, max> $backoffMaxTries
     * @param HttpClientInterface|null $httpClient Test/integration seam: inject a pre-built inner
     *     client (e.g. a {@see \Symfony\Component\HttpClient\MockHttpClient}). When null, the
     *     {@see ApiClient} builds one via {@see \Symfony\Component\HttpClient\HttpClient::create()}.
     */
    public function __construct(
        public readonly string $userAgent = 'Keboola PHP API Client',
        public readonly int $backoffMaxTries = self::DEFAULT_BACKOFF_MAX_TRIES,
        public readonly int $connectTimeout = self::DEFAULT_CONNECT_TIMEOUT,
        public readonly int $requestTimeout = self::DEFAULT_REQUEST_TIMEOUT,
        public readonly ?HttpClientInterface $httpClient = null,
        public readonly ?LoggerInterface $logger = null,
    ) {
    }
}
