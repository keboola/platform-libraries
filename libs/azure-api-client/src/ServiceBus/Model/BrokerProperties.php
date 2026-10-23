<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\ServiceBus\Model;

use DateTimeImmutable;
use DateTimeZone;
use InvalidArgumentException;
use Webmozart\Assert\Assert;

/**
 * Subset of the broker properties Service Bus returns with a received message.
 * https://learn.microsoft.com/en-us/rest/api/servicebus/message-headers-and-properties
 */
final class BrokerProperties
{
    public const DATE_FORMAT = 'D, d M Y H:i:s \G\M\T';

    public function __construct(
        public readonly string $messageId,
        public readonly int $deliveryCount,
        public readonly int $sequenceNumber,
        public readonly DateTimeImmutable $enqueuedTimeUtc,
        public readonly ?DateTimeImmutable $scheduledEnqueueTimeUtc,
        public readonly ?DateTimeImmutable $lockedUntilUtc,
        public readonly ?string $lockToken,
    ) {
    }

    public static function fromResponseData(array $data): self
    {
        Assert::keyExists($data, 'MessageId');
        Assert::string($data['MessageId']);
        Assert::keyExists($data, 'DeliveryCount');
        Assert::integer($data['DeliveryCount']);
        Assert::keyExists($data, 'SequenceNumber');
        Assert::integer($data['SequenceNumber']);
        Assert::keyExists($data, 'EnqueuedTimeUtc');
        Assert::nullOrString($data['LockToken'] ?? null);

        return new self(
            messageId: $data['MessageId'],
            deliveryCount: $data['DeliveryCount'],
            sequenceNumber: $data['SequenceNumber'],
            enqueuedTimeUtc: self::parseDate($data['EnqueuedTimeUtc']),
            scheduledEnqueueTimeUtc: self::parseOptionalDate($data, 'ScheduledEnqueueTimeUtc'),
            lockedUntilUtc: self::parseOptionalDate($data, 'LockedUntilUtc'),
            lockToken: $data['LockToken'] ?? null,
        );
    }

    public static function formatDate(DateTimeImmutable $date): string
    {
        return $date->setTimezone(new DateTimeZone('UTC'))->format(self::DATE_FORMAT);
    }

    private static function parseOptionalDate(array $data, string $key): ?DateTimeImmutable
    {
        if (!isset($data[$key])) {
            return null;
        }
        return self::parseDate($data[$key]);
    }

    private static function parseDate(mixed $value): DateTimeImmutable
    {
        Assert::string($value);
        $date = DateTimeImmutable::createFromFormat(self::DATE_FORMAT, $value, new DateTimeZone('UTC'));
        // the round trip rejects values PHP silently adjusts, e.g. a weekday that does not match the date
        if ($date === false || $date->format(self::DATE_FORMAT) !== $value) {
            throw new InvalidArgumentException(sprintf('Invalid broker properties date "%s"', $value));
        }
        return $date;
    }
}
