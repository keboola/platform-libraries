<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\AuthBridge\Exception;

/**
 * The subject token cannot access the requested project, or the caller is not allowed to use
 * the resolver (Connection resolver returned 403).
 */
final class ProjectAccessDeniedException extends StorageTokenResolverException
{
}
