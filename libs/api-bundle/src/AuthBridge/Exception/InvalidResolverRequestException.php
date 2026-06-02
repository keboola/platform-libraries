<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\AuthBridge\Exception;

/**
 * The resolver request was malformed, e.g. a missing or invalid project id
 * (Connection resolver returned 400).
 */
final class InvalidResolverRequestException extends StorageTokenResolverException
{
}
