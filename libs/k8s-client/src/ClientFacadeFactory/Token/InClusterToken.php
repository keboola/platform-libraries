<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFacadeFactory\Token;

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
