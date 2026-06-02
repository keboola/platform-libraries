<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\AuthBridge\Exception;

/**
 * The resolver could not be reached or returned a server-side error / timeout
 * (network error or Connection resolver 5xx).
 */
final class ResolverUnavailableException extends StorageTokenResolverException
{
}
