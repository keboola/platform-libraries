<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Marketplace\Model;

use Keboola\AzureApiClient\Marketplace\Model\ReportUsageEventsBatchResult;
use Keboola\AzureApiClient\Marketplace\Model\UsageEventResult;
use PHPUnit\Framework\TestCase;

class ReportUsageEventsBatchResultTest extends TestCase
{
    public function testFromResponseData(): void
    {
        // https://learn.microsoft.com/en-us/azure/marketplace/marketplace-metering-service-apis#responses-1
        $responseData = [
            'count' => 2,
            'result' => [
                [
                    'usageEventId' => '<guid>',
                    'status' => 'Accepted',
                    'messageTime' => '2020-01-12T13:19:35.3458658Z',
                    'resourceId' => '<guid1>',
                    'quantity' => 5,
                    'dimension' => 'dim1',
                    'effectiveStartTime' => '2018-12-01T08:30:14',
                    'planId' => 'plan1',
                ],
                [
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
                    'quantity' => 1,
                    'dimension' => 'email',
                    'effectiveStartTime' => '2020-01-12T11:03:28.14Z',
                    'planId' => 'gold',
                ],
            ],
        ];

        $result = ReportUsageEventsBatchResult::fromResponseData($responseData);

        self::assertCount(2, $result->result);
        self::assertEquals(UsageEventResult::fromResponseData($responseData['result'][0]), $result->result[0]);
        self::assertEquals(UsageEventResult::fromResponseData($responseData['result'][1]), $result->result[1]);
    }
}
