<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient;

use Closure;
use GuzzleHttp\HandlerStack;
use Keboola\GitServiceApiClient\Auth\AuthInterface;
use Keboola\GitServiceApiClient\Auth\KeboolaServiceAccountAuth;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class ApiClientConfiguration
{
    private const DEFAULT_BACKOFF_RETRIES = 5;

    /**
     * @param int<0, max> $backoffMaxTries
     * @param AuthInterface $auth
     *   How the client authenticates against git-service. Defaults to a
     *   {@see KeboolaServiceAccountAuth} pointing at the standard
     *   in-cluster SA token path, which reads (and re-reads) the file on
     *   every request and throws if the file is missing or empty. Pass
     *   {@see Auth\ManageApiTokenAuth} for a Manage API token, or
     *   {@see KeboolaServiceAccountAuth} with a custom path for a
     *   non-default projected-token mount.
     */
    public function __construct(
        public readonly AuthInterface $auth = new KeboolaServiceAccountAuth(),
        public readonly ?string $userAgent = null,
        public readonly int $backoffMaxTries = self::DEFAULT_BACKOFF_RETRIES,
        public readonly null|Closure|HandlerStack $requestHandler = null,
        public readonly LoggerInterface $logger = new NullLogger(),
    ) {
    }
}
