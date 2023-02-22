<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Marketplace\Model;

use DateTimeImmutable;
use Keboola\AzureApiClient\Marketplace\Model\UsageEventError;
use Keboola\AzureApiClient\Marketplace\Model\UsageEventResult;
use PHPUnit\Framework\TestCase;

class UsageEventResultTest extends TestCase
{
    public function testFromResponseData(): void
    {
        // https://learn.microsoft.com/en-us/azure/marketplace/marketplace-metering-service-apis#responses-1
        $responseData = [
            'usageEventId' => '<guid>',
            'status' => 'Accepted',
            'messageTime' => '2020-01-12T13:19:35.3458658Z',
            'resourceId' => '<guid1>',
            'quantity' => 5.0,
            'dimension' => 'dim1',
            'effectiveStartTime' => '2018-12-01T08:30:14',
            'planId' => 'plan1',
        ];

        $result = UsageEventResult::fromResponseData($responseData);

        self::assertSame('<guid>', $result->usageEventId);
        self::assertNull($result->error);
        self::assertSame('Accepted', $result->status);
        self::assertEquals(new DateTimeImmutable('2020-01-12T13:19:35.3458658Z'), $result->messageTime);
        self::assertSame('<guid1>', $result->resourceId);
        self::assertSame('plan1', $result->planId);
        self::assertSame('dim1', $result->dimension);
        self::assertSame(5.0, $result->quantity);
        self::assertEquals(new DateTimeImmutable('2018-12-01T08:30:14'), $result->effectiveStartTime);
    }

    public function testFromResponseDataWithError(): void
    {
        // https://learn.microsoft.com/en-us/azure/marketplace/marketplace-metering-service-apis#responses-1
        $responseData = [
            'status' => 'Duplicate',
            'messageTime' => '0001-01-01T00:00:00',
            'error' => [
                'additionalInfo' => [
                    'acceptedMessage' => [
                        'usageEventId' => '<guid>',
                        'status' => 'Duplicate',
                        'messageTime' => '2020-01-12T13:19:35.3458658Z',
                        'resourceId' => '<guid2>',
                        'quantity' => 1,
                        'dimension' => 'email',
                        'effectiveStartTime' => '2020-01-12T11:03:28.14Z',
                        'planId' => 'gold',
                    ],
                ],
                'message' => 'This usage event already exist.',
                'code' => 'Conflict',
            ],
            'resourceId' => '<guid2>',
            'quantity' => '1',
            'dimension' => 'email',
            'effectiveStartTime' => '2020-01-12T11:03:28.14Z',
            'planId' => 'gold',
        ];

        $result = UsageEventResult::fromResponseData($responseData);

        self::assertNull($result->usageEventId);
        self::assertEquals(UsageEventError::fromResponseData([
            'additionalInfo' => [
                'acceptedMessage' => [
                    'usageEventId' => '<guid>',
                    'status' => 'Duplicate',
                    'messageTime' => '2020-01-12T13:19:35.3458658Z',
                    'resourceId' => '<guid2>',
                    'quantity' => 1,
                    'dimension' => 'email',
                    'effectiveStartTime' => '2020-01-12T11:03:28.14Z',
                    'planId' => 'gold',
                ],
            ],
            'message' => 'This usage event already exist.',
            'code' => 'Conflict',
        ]), $result->error);
        self::assertSame('Duplicate', $result->status);
        self::assertEquals(new DateTimeImmutable('0001-01-01T00:00:00'), $result->messageTime);
        self::assertSame('<guid2>', $result->resourceId);
        self::assertSame('gold', $result->planId);
        self::assertSame('email', $result->dimension);
        self::assertSame(1.0, $result->quantity);
        self::assertEquals(new DateTimeImmutable('2020-01-12T11:03:28.14Z'), $result->effectiveStartTime);
    }
}
