<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\AuthBridge;

use Keboola\ApiBundle\AuthBridge\Exception\ResolverUnavailableException;

/**
 * Reads the projected Kubernetes ServiceAccount token that identifies this service to
 * Connection's internal auth-bridge resolver.
 *
 * The token file is re-read on every call so kubelet-rotated tokens are picked up
 * automatically. A missing, unreadable, or empty file is treated as a deployment/identity
 * problem on our side and surfaces as a {@see ResolverUnavailableException}. The token value
 * itself is never logged nor placed into any exception message.
 */
class KubernetesServiceAccountTokenProvider
{
    public const DEFAULT_TOKEN_PATH = '/var/run/secrets/connection.keboola.com/serviceaccount/token';

    public function __construct(
        private readonly string $tokenPath = self::DEFAULT_TOKEN_PATH,
    ) {
    }

    /**
     * @throws ResolverUnavailableException
     */
    public function getToken(): string
    {
        if (!is_readable($this->tokenPath)) {
            throw new ResolverUnavailableException(sprintf(
                'Service account token file "%s" is not readable.',
                $this->tokenPath,
            ));
        }

        $token = file_get_contents($this->tokenPath);
        if ($token === false) {
            throw new ResolverUnavailableException(sprintf(
                'Failed to read service account token file "%s".',
                $this->tokenPath,
            ));
        }

        $token = trim($token);
        if ($token === '') {
            throw new ResolverUnavailableException(sprintf(
                'Service account token file is empty: "%s".',
                $this->tokenPath,
            ));
        }

        return $token;
    }
}
