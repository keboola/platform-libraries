<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\ServiceBus\Model;

use DateTimeImmutable;
use Keboola\AzureApiClient\ServiceBus\ServiceBusApiClient;
use Webmozart\Assert\Assert;

class BrokerProperties
{
    public function __construct(
        public readonly int $deliveryCount,
        public readonly DateTimeImmutable $scheduledEnqueueTimeUtc,
        public readonly string $messageId,
        public readonly int $sequenceNumber,
    ) {
    }

    public static function fromResponseData(array $brokerProperties): self
    {
        Assert::keyExists($brokerProperties, 'DeliveryCount');
        $deliveryCount = (int) $brokerProperties['DeliveryCount'];

        Assert::keyExists($brokerProperties, 'MessageId');
        $messageId = $brokerProperties['MessageId'];

        Assert::keyExists($brokerProperties, 'SequenceNumber');
        $sequenceNumber = (int) $brokerProperties['SequenceNumber'];

        Assert::keyExists($brokerProperties, 'EnqueuedTimeUtc');
        $scheduledEnqueueTimeUtc = \DateTimeImmutable::createFromFormat(
            ServiceBusApiClient::AZURE_DATE_FORMAT,
            $brokerProperties['EnqueuedTimeUtc']
        );

        // other broker properties are not used now

//        if (array_key_exists('LockedUntilUtc', $brokerProperties)) {
//            $LockedUntilUtc = \DateTimeImmutable::createFromFormat(
//                ServiceBusApiClient::AZURE_DATE_FORMAT,
//                $brokerProperties['LockedUntilUtc']
//            );
//        }
//
//        if (array_key_exists('LockToken', $brokerProperties)) {
//            $LockToken = $brokerProperties['LockToken'];
//        }
//
//        if (array_key_exists('Label', $brokerProperties)) {
//            $Label = $brokerProperties['Label'];
//        }
//
//        if (array_key_exists('ReplyTo', $brokerProperties)) {
//            $ReplyTo = $brokerProperties['ReplyTo'];
//        }
//
//
//        if (array_key_exists('TimeToLive', $brokerProperties)) {
//            $TimeToLive = (float) $brokerProperties['TimeToLive'];
//        }
//
//        if (array_key_exists('To', $brokerProperties)) {
//            $To = $brokerProperties['To'];
//        }
//
//
//        if (array_key_exists('ReplyToSessionId', $brokerProperties)) {
//            $ReplyToSessionId = $brokerProperties['ReplyToSessionId'];
//        }
//
//        if (array_key_exists('MessageLocation', $brokerProperties)) {
//            $MessageLocation = $brokerProperties['MessageLocation'];
//        }
//
//        if (array_key_exists('LockLocation', $brokerProperties)) {
//            $LockLocation = $brokerProperties['LockLocation'];
//        }

        return new self(
            deliveryCount: $deliveryCount,
            scheduledEnqueueTimeUtc: $scheduledEnqueueTimeUtc,
            messageId: $messageId,
            sequenceNumber: $sequenceNumber,
        );
    }
}
