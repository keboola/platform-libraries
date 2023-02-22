<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Marketplace\Model;

use Keboola\AzureApiClient\Marketplace\Model\UsageEventError;
use PHPUnit\Framework\TestCase;

class UsageEventErrorTest extends TestCase
{
    public function testFromResponseData(): void
    {
        // https://learn.microsoft.com/en-us/azure/marketplace/marketplace-metering-service-apis#responses
        $responseData = [
            'message' => 'One or more errors have occurred.',
            'target' => 'usageEventRequest',
            'details' => [
                [
                    'message' => 'The resourceId is required.',
                    'target' => 'ResourceId',
                    'code' => 'BadArgument',
                ],
            ],
            'code' => 'BadArgument',
        ];

        $result = UsageEventError::fromResponseData($responseData);

        self::assertSame('BadArgument', $result->code);
        self::assertSame('One or more errors have occurred.', $result->message);
        self::assertNull($result->additionalInfo);
    }

    public function testFromResponseDataWithAdditionalInfo(): void
    {
        // https://learn.microsoft.com/en-us/azure/marketplace/marketplace-metering-service-apis#responses
        $responseData = [
            'additionalInfo' => [
                'acceptedMessage' => [
                    'usageEventId' => '<guid>',
                    'status' => 'Duplicate',
                    'messageTime' => '2020-01-12T13:19:35.3458658Z',
                    'resourceId' => '<guid>',
                    'quantity' => 1,
                    'dimension' => 'dim1',
                    'effectiveStartTime' => '2020-01-12T11:03:28.14Z',
                    'planId' => 'plan1',
                ],
            ],
            'message' => 'This usage event already exist.',
            'code' => 'Conflict',
        ];

        $result = UsageEventError::fromResponseData($responseData);

        self::assertSame('Conflict', $result->code);
        self::assertSame('This usage event already exist.', $result->message);
        self::assertSame([
            'acceptedMessage' => [
                'usageEventId' => '<guid>',
                'status' => 'Duplicate',
                'messageTime' => '2020-01-12T13:19:35.3458658Z',
                'resourceId' => '<guid>',
                'quantity' => 1,
                'dimension' => 'dim1',
                'effectiveStartTime' => '2020-01-12T11:03:28.14Z',
                'planId' => 'plan1',
            ],
        ], $result->additionalInfo);
    }
}
