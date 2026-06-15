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
    /** @phpstan-ignore classConstant.unused (used in Task 3 readToken retry loop) */
    private const MAX_RETRY_DELAY_US = 1_000_000;

    /**
     * @param non-empty-string $tokenPath
     * @param positive-int $maxReadAttempts Total reads (1 initial + N-1 retries) before giving up.
     * @param int<0, max> $retryBaseDelayMicroseconds Base backoff, doubled each retry, capped at 1 s.
     */
    public function __construct(
        private string $tokenPath = self::DEFAULT_TOKEN_PATH,
        /** @phpstan-ignore property.onlyWritten (used in Task 3 readToken retry loop) */
        private int $maxReadAttempts = self::DEFAULT_MAX_READ_ATTEMPTS,
        /** @phpstan-ignore property.onlyWritten (used in Task 3 readToken retry loop) */
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
        if (!is_readable($this->tokenPath)) {
            throw new RuntimeException(sprintf(
                'Service account token file "%s" is not readable',
                $this->tokenPath,
            ));
        }

        $token = file_get_contents($this->tokenPath);
        if ($token === false) {
            throw new RuntimeException(sprintf(
                'Failed to read service account token file "%s"',
                $this->tokenPath,
            ));
        }

        $token = trim($token);
        if ($token === '') {
            throw new RuntimeException(sprintf(
                'Service account token file is empty: "%s"',
                $this->tokenPath,
            ));
        }

        return $token;
    }
}
