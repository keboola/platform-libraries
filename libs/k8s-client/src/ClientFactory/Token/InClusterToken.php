<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFactory\Token;

use Keboola\K8sClient\Exception\ConfigurationException;

class InClusterToken implements TokenInterface
{
    // K8S automatically refreshes the token some time before it expires
    // the time is not documented, but from testing we know it's around 10 minutes before actual expiration
    public const DEFAULT_EXPIRATION_TIME = 5 * 60; // 5 minutes

    private int $lastRefreshTime = 0;
    private ?string $cachedValue = null;

    public function __construct(
        private readonly string $tokenFilePath,
        private readonly int $expirationTime = self::DEFAULT_EXPIRATION_TIME,
    ) {
    }

    public function getValue(): string
    {
        $currentTime = time();
        $secondsSinceRefresh = $currentTime - $this->lastRefreshTime;

        if ($this->cachedValue === null || $secondsSinceRefresh >= $this->expirationTime) {
            $fileContents = @file_get_contents($this->tokenFilePath);

            if ($fileContents === false) {
                // K8S rotates projected service account tokens by atomically swapping the "..data" symlink,
                // so the read can transiently fail while the previously read token is still valid
                // (K8S refreshes the token ~10 minutes before its expiration)
                if ($this->cachedValue !== null) {
                    // $lastRefreshTime is intentionally not updated, so that the next call retries the read
                    // instead of serving the cached value for another full expiration period
                    return $this->cachedValue;
                }

                throw new ConfigurationException(sprintf(
                    'Failed to read contents of in-cluster configuration file "%s"',
                    $this->tokenFilePath,
                ));
            }

            $this->cachedValue = trim($fileContents);
            $this->lastRefreshTime = $currentTime;
        }

        return $this->cachedValue;
    }
}
