<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Auth;

use Psr\Http\Message\RequestInterface;
use RuntimeException;
use Webmozart\Assert\Assert;

/**
 * Authenticates via a projected Kubernetes ServiceAccount token mounted by the
 * kbc-stacks chart at {@see self::DEFAULT_TOKEN_PATH}. The file is re-read on
 * every request so kubelet-rotated tokens are picked up automatically.
 */
final readonly class KeboolaServiceAccountAuthenticator implements RequestAuthenticatorInterface
{
    public const HEADER = 'X-Kubernetes-Authorization';
    public const DEFAULT_TOKEN_PATH = '/var/run/secrets/connection.keboola.com/serviceaccount/token';

    public const DEFAULT_MAX_READ_ATTEMPTS = 6;
    public const DEFAULT_RETRY_BASE_DELAY_US = 40_000;
    private const MAX_RETRY_DELAY_US = 1_000_000;

    /**
     * @param non-empty-string $tokenPath
     * @param positive-int $maxReadAttempts Total reads (1 initial + N-1 retries) before giving up.
     * @param int<0, max> $retryBaseDelayMicroseconds Base backoff, doubled each retry, capped at 1 s.
     */
    public function __construct(
        private string $tokenPath = self::DEFAULT_TOKEN_PATH,
        private int $maxReadAttempts = self::DEFAULT_MAX_READ_ATTEMPTS,
        private int $retryBaseDelayMicroseconds = self::DEFAULT_RETRY_BASE_DELAY_US,
    ) {
        Assert::stringNotEmpty($tokenPath, 'Service account token path must not be empty');
        Assert::greaterThanEq($maxReadAttempts, 1, 'maxReadAttempts must be at least 1');
        Assert::greaterThanEq($retryBaseDelayMicroseconds, 0, 'retryBaseDelayMicroseconds must not be negative');
    }

    public function __invoke(RequestInterface $request): RequestInterface
    {
        return $request->withHeader(self::HEADER, 'Bearer ' . $this->readToken());
    }

    /**
     * @return non-empty-string
     */
    private function readToken(): string
    {
        $attempt = 0;
        while (true) {
            $raw = @file_get_contents($this->tokenPath);
            if ($raw === false) {
                // A failed read may be a transient symlink swap during kubelet
                // rotation. Clear PHP's (process-global) stat cache so a stale
                // entry can't mask the freshly rotated file, then re-check.
                $this->clearStatCache();
                if (!is_readable($this->tokenPath)) {
                    throw new RuntimeException(sprintf(
                        'Service account token file "%s" is not readable',
                        $this->tokenPath,
                    ));
                }
                $raw = @file_get_contents($this->tokenPath);
            }

            $token = $raw === false ? '' : trim($raw);
            if ($token !== '') {
                return $token;
            }

            if (++$attempt >= $this->maxReadAttempts) {
                throw new RuntimeException(sprintf(
                    'Service account token file is empty: "%s"',
                    $this->tokenPath,
                ));
            }

            usleep($this->backoffMicroseconds($attempt));
            $this->clearStatCache();
        }
    }

    private function backoffMicroseconds(int $attempt): int
    {
        $exponent = $attempt > 1 ? $attempt - 1 : 0;
        $delay = $this->retryBaseDelayMicroseconds * (2 ** $exponent);

        return max(0, (int) min($delay, self::MAX_RETRY_DELAY_US));
    }

    /**
     * Clears the stat cache for the token path and, when it is a symlink (the
     * projected SA token is one), for the link target and its directory too —
     * mirroring the AWS SDK, but guarded by is_link() so it is safe for plain
     * file paths.
     */
    private function clearStatCache(): void
    {
        clearstatcache(true, $this->tokenPath);
        if (is_link($this->tokenPath)) {
            $target = @readlink($this->tokenPath);
            if ($target !== false && $target !== '') {
                $resolved = str_starts_with($target, '/')
                    ? $target
                    : dirname($this->tokenPath) . '/' . $target;
                clearstatcache(true, $resolved);
                clearstatcache(true, dirname($resolved));
            }
        }
    }
}
