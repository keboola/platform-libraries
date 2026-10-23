<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\ServiceBus\Exception;

use Keboola\AzureApiClient\Exception\ClientException;

/**
 * The lock of a peek-locked message is no longer held: it expired, or the message was already deleted.
 * The message may have been delivered to another receiver.
 */
class MessageLockLostException extends ClientException
{
}
