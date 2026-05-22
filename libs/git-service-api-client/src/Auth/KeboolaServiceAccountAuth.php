<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Auth;

use RuntimeException;
use Webmozart\Assert\Assert;

/**
 * Authenticates against git-service via a projected Kubernetes ServiceAccount
 * token mounted by the kbc-stacks chart at {@see self::DEFAULT_TOKEN_PATH}.
 * The file is re-read on every request so kubelet-rotated tokens are picked up
 * automatically.
 */
final readonly class KeboolaServiceAccountAuth implements AuthInterface
{
    public const DEFAULT_TOKEN_PATH = '/var/run/secrets/connection.keboola.com/serviceaccount/token';

    /**
     * @param non-empty-string $tokenPath
     */
    public function __construct(private string $tokenPath = self::DEFAULT_TOKEN_PATH)
    {
        Assert::stringNotEmpty($tokenPath, 'Service account token path must not be empty');
    }

    public function getAuthenticationHeaders(): array
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

        return [
            'X-Kubernetes-Authorization' => 'Bearer ' . $token,
        ];
    }
}
