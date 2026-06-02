<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\AuthBridge\Exception;

/**
 * The subject token is missing, malformed, expired, revoked, or otherwise invalid
 * (Connection resolver returned 401).
 */
final class UnauthorizedSubjectTokenException extends StorageTokenResolverException
{
}
