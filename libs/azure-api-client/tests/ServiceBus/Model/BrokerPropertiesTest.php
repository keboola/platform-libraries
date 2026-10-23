<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\ServiceBus\Model;

use DateTimeImmutable;
use Generator;
use InvalidArgumentException;
use Keboola\AzureApiClient\ServiceBus\Model\BrokerProperties;
use PHPUnit\Framework\TestCase;

class BrokerPropertiesTest extends TestCase
{
    public function testFromResponseDataWithAllProperties(): void
    {
        $properties = BrokerProperties::fromResponseData([
            'DeliveryCount' => 3,
            'EnqueuedSequenceNumber' => 0,
            'EnqueuedTimeUtc' => 'Wed, 02 Jul 2014 01:32:27 GMT',
            'ScheduledEnqueueTimeUtc' => 'Wed, 02 Jul 2014 01:30:00 GMT',
            'LockToken' => 'lock-token',
            'LockedUntilUtc' => 'Wed, 02 Jul 2014 01:33:27 GMT',
            'MessageId' => 'message-id',
            'SequenceNumber' => 42,
            'State' => 'Active',
        ]);

        self::assertSame('message-id', $properties->messageId);
        self::assertSame(3, $properties->deliveryCount);
        self::assertSame(42, $properties->sequenceNumber);
        self::assertEquals(new DateTimeImmutable('2014-07-02T01:32:27Z'), $properties->enqueuedTimeUtc);
        self::assertSame('UTC', $properties->enqueuedTimeUtc->getTimezone()->getName());
        self::assertEquals(new DateTimeImmutable('2014-07-02T01:30:00Z'), $properties->scheduledEnqueueTimeUtc);
        self::assertEquals(new DateTimeImmutable('2014-07-02T01:33:27Z'), $properties->lockedUntilUtc);
        self::assertSame('lock-token', $properties->lockToken);
    }

    public function testFromResponseDataWithRequiredPropertiesOnly(): void
    {
        $properties = BrokerProperties::fromResponseData([
            'DeliveryCount' => 1,
            'EnqueuedTimeUtc' => 'Wed, 02 Jul 2014 01:32:27 GMT',
            'MessageId' => 'message-id',
            'SequenceNumber' => 2,
        ]);

        self::assertNull($properties->scheduledEnqueueTimeUtc);
        self::assertNull($properties->lockedUntilUtc);
        self::assertNull($properties->lockToken);
    }

    /** @dataProvider provideInvalidData */
    public function testFromResponseDataWithInvalidData(array $data, string $expectedError): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($expectedError);

        BrokerProperties::fromResponseData($data);
    }

    public static function provideInvalidData(): Generator
    {
        $valid = [
            'DeliveryCount' => 1,
            'EnqueuedTimeUtc' => 'Wed, 02 Jul 2014 01:32:27 GMT',
            'MessageId' => 'message-id',
            'SequenceNumber' => 2,
        ];

        foreach (array_keys($valid) as $key) {
            $data = $valid;
            unset($data[$key]);
            yield 'missing ' . $key => [
                'data' => $data,
                'expectedError' => sprintf('Expected the key "%s" to exist.', $key),
            ];
        }

        yield 'non-string MessageId' => [
            'data' => ['MessageId' => 1] + $valid,
            'expectedError' => 'Expected a string. Got: integer',
        ];

        yield 'non-integer DeliveryCount' => [
            'data' => ['DeliveryCount' => '1'] + $valid,
            'expectedError' => 'Expected an integer. Got: string',
        ];

        yield 'non-integer SequenceNumber' => [
            'data' => ['SequenceNumber' => '2'] + $valid,
            'expectedError' => 'Expected an integer. Got: string',
        ];

        yield 'non-string LockToken' => [
            'data' => ['LockToken' => 1] + $valid,
            'expectedError' => 'Expected a string. Got: integer',
        ];

        yield 'invalid date' => [
            'data' => ['EnqueuedTimeUtc' => '2014-07-02T01:32:27Z'] + $valid,
            'expectedError' => 'Invalid broker properties date "2014-07-02T01:32:27Z"',
        ];

        yield 'weekday not matching the date' => [
            'data' => ['EnqueuedTimeUtc' => 'Mon, 02 Jul 2014 01:32:27 GMT'] + $valid,
            'expectedError' => 'Invalid broker properties date "Mon, 02 Jul 2014 01:32:27 GMT"',
        ];

        yield 'non-string date' => [
            'data' => ['LockedUntilUtc' => 1] + $valid,
            'expectedError' => 'Expected a string. Got: integer',
        ];
    }

    public function testFormatDateConvertsToUtc(): void
    {
        self::assertSame(
            'Tue, 05 Mar 2024 12:15:16 GMT',
            BrokerProperties::formatDate(new DateTimeImmutable('2024-03-05T14:15:16+02:00')),
        );
    }
}
