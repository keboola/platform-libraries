<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Exception;

use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\PermissionChecker\Feature;
use Keboola\PermissionChecker\Role;
use RuntimeException;

class PermissionDeniedException extends RuntimeException implements UserExceptionInterface
{
    public static function missingFeature(Feature $feature): self
    {
        return new self(sprintf('Project does not have feature "%s" enabled', $feature->value));
    }

    public static function missingComponent(string $componentId): self
    {
        return new self(sprintf('Token is not allowed to run component "%s"', $componentId));
    }

    public static function roleDenied(Role $role, string $action): self
    {
        return new self(sprintf('Role "%s" is not allowed to %s', $role->value, $action));
    }
}
