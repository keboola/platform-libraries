<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\AuthBridge;

use Keboola\ApiBundle\AuthBridge\Exception\StorageTokenResolverException;
use SensitiveParameter;

/**
 * Resolves a Connection programmatic bearer token (kbc_at_* / kbc_pat_*) into a legacy
 * Storage token for a given project, via Connection's internal auth-bridge resolver.
 *
 * This is the stable seam that lets the concrete implementation move to the Manage API
 * client later without touching the authenticators.
 */
interface StorageTokenResolverInterface
{
    /**
     * @param int $projectId Project whose Storage token should be resolved (expected to be > 0).
     * @param string $subjectToken Bare programmatic token (without the "Bearer " scheme).
     * @throws StorageTokenResolverException
     */
    public function resolve(int $projectId, #[SensitiveParameter] string $subjectToken): ResolvedStorageToken;
}
