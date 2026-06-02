<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\AuthBridge\Exception;

use RuntimeException;

/**
 * Base exception for all failures of the storage token resolver. Messages must never contain
 * the subject token or the resolved legacy Storage token.
 */
class StorageTokenResolverException extends RuntimeException
{
}
