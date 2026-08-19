<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFactory\Token;

use Keboola\K8sClient\Exception\ConfigurationException;

class InClusterToken implements TokenInterface
{
    // K8S automatically refreshes the token some time before it expires
    // the time is not documented, but from testing we know it's around 10 minutes before actual expiration
    public const DEFAULT_EXPIRATION_TIME = 5 * 60; // 5 minutes

    public const DEFAULT_MAX_READ_ATTEMPTS = 6;
    public const DEFAULT_RETRY_BASE_DELAY_US = 40_000; // 40 ms, doubled on each retry => ~1.24 s total
    private const MAX_RETRY_DELAY_US = 1_000_000;

    private int $lastRefreshTime = 0;
    private ?string $cachedValue = null;

    /**
     * @param int $maxReadAttempts Total reads (1 initial + N-1 retries) before giving up. Must be at least 1.
     * @param int $retryBaseDelayMicroseconds Base backoff, doubled on each retry, capped at 1 s. Must not be negative.
     */
    public function __construct(
        private readonly string $tokenFilePath,
        private readonly int $expirationTime = self::DEFAULT_EXPIRATION_TIME,
        private readonly int $maxReadAttempts = self::DEFAULT_MAX_READ_ATTEMPTS,
        private readonly int $retryBaseDelayMicroseconds = self::DEFAULT_RETRY_BASE_DELAY_US,
    ) {
        if ($maxReadAttempts < 1) {
            throw new ConfigurationException('Maximum number of token read attempts must be at least 1');
        }

        if ($retryBaseDelayMicroseconds < 0) {
            throw new ConfigurationException('Token read retry base delay must not be negative');
        }
    }

    public function getValue(): string
    {
        $currentTime = time();
        $cachedValue = $this->cachedValue;

        if ($cachedValue !== null && $currentTime - $this->lastRefreshTime < $this->expirationTime) {
            return $cachedValue;
        }

        try {
            $token = $this->readTokenFile();
        } catch (ConfigurationException $e) {
            // K8S rotates projected service account tokens by atomically swapping the "..data" symlink,
            // so a read can fail or return an empty file while the previously read token is still valid
            // (K8S refreshes the token ~10 minutes before its expiration)
            if ($cachedValue === null) {
                throw $e;
            }

            // $lastRefreshTime is intentionally not updated, so that the next call retries the read
            // instead of serving the cached value for another full expiration period
            return $cachedValue;
        }

        $this->cachedValue = $token;
        $this->lastRefreshTime = $currentTime;

        return $token;
    }

    /**
     * @return non-empty-string
     */
    private function readTokenFile(): string
    {
        $attempt = 0;

        while (true) {
            $fileContents = @file_get_contents($this->tokenFilePath);
            $token = $fileContents === false ? '' : trim($fileContents);

            if ($token !== '') {
                return $token;
            }

            $this->clearStatCache();

            // a file that can't be read at all is not a rotation glitch, so the retry budget is not spent on it
            if (!is_readable($this->tokenFilePath)) {
                throw new ConfigurationException(sprintf(
                    'Failed to read contents of in-cluster configuration file "%s"',
                    $this->tokenFilePath,
                ));
            }

            if (++$attempt >= $this->maxReadAttempts) {
                throw new ConfigurationException(sprintf(
                    'In-cluster configuration file "%s" is empty or unreadable after %d attempts',
                    $this->tokenFilePath,
                    $this->maxReadAttempts,
                ));
            }

            usleep($this->backoffMicroseconds($attempt));
            $this->clearStatCache();
        }
    }

    private function backoffMicroseconds(int $attempt): int
    {
        $delay = $this->retryBaseDelayMicroseconds * (2 ** ($attempt - 1));

        return max(0, (int) min($delay, self::MAX_RETRY_DELAY_US));
    }

    /**
     * Clears the stat cache for the token path and, when it is a symlink (the projected SA token is one),
     * for the link target and its directory too, so that a stale entry can't mask a freshly rotated file.
     */
    private function clearStatCache(): void
    {
        clearstatcache(true, $this->tokenFilePath);

        if (!is_link($this->tokenFilePath)) {
            return;
        }

        $target = @readlink($this->tokenFilePath);
        if ($target === false || $target === '') {
            return;
        }

        $resolvedTarget = str_starts_with($target, '/')
            ? $target
            : dirname($this->tokenFilePath).'/'.$target;

        clearstatcache(true, $resolvedTarget);
        clearstatcache(true, dirname($resolvedTarget));
    }
}
