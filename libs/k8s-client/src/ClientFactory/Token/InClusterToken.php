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
    public const DEFAULT_RETRY_BASE_DELAY_US = 40_000;
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
        $secondsSinceRefresh = $currentTime - $this->lastRefreshTime;

        if ($this->cachedValue === null || $secondsSinceRefresh >= $this->expirationTime) {
            $this->cachedValue = $this->readTokenFile();
            $this->lastRefreshTime = $currentTime;
        }

        return $this->cachedValue;
    }

    /**
     * @return non-empty-string
     */
    private function readTokenFile(): string
    {
        $attempt = 0;

        while (true) {
            $this->clearStatCache();
            $fileContents = @file_get_contents($this->tokenFilePath);
            $token = $fileContents === false ? '' : trim($fileContents);

            if ($token !== '') {
                return $token;
            }

            $this->clearStatCache();

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
        }
    }

    private function backoffMicroseconds(int $attempt): int
    {
        $delay = $this->retryBaseDelayMicroseconds * (2 ** ($attempt - 1));

        return max(0, (int) min($delay, self::MAX_RETRY_DELAY_US));
    }

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
