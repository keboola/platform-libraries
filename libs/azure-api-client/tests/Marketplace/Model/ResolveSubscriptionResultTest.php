<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Marketplace\Model;

use Keboola\AzureApiClient\Marketplace\Model\ResolveSubscriptionResult;
use Keboola\AzureApiClient\Marketplace\Model\Subscription;
use PHPUnit\Framework\TestCase;

class ResolveSubscriptionResultTest extends TestCase
{
    public function testFromResponseData(): void
    {
        // example data from https://learn.microsoft.com/en-us/azure/marketplace/partner-center-portal/pc-saas-fulfillment-subscription-api#post-httpsmarketplaceapimicrosoftcomapisaassubscriptionsresolveapi-versionapiversion
        $responseData = [
            'id' => '<guid>',
            'subscriptionName' => 'Contoso Cloud Solution',
            'offerId' => 'offer1',
            'planId' => 'silver',
            'quantity' => 20,
            'subscription' => [
                'id' => '<guid>',
                'publisherId' => 'contoso',
                'offerId' => 'offer1',
                'name' => 'Contoso Cloud Solution',
                'saasSubscriptionStatus' => ' PendingFulfillmentStart ',
                'beneficiary' => [
                    'emailId' => 'test@test.com',
                    'objectId' => '<guid>',
                    'tenantId' => '<guid>',
                    'puid' => '<ID of the user>',
                ],
                'purchaser' => [
                    'emailId' => 'test@test.com',
                    'objectId' => '<guid>',
                    'tenantId' => '<guid>',
                    'puid' => '<ID of the user>',
                ],
                'planId' => 'silver',
                'term' => [
                    'termUnit' => 'P1M',
                    'startDate' => '2022-03-07T00:00:00Z',
                    'endDate' => '2022-04-06T00:00:00Z',
                ],
                'autoRenew' => true,
                'isTest' => true,
                'isFreeTrial' => false,
                'allowedCustomerOperations' => [
                    'Delete',
                    'Update',
                    'Read',
                ],
                'sandboxType' => 'None',
                'lastModified' => '0001-01-01T00:00:00',
                'quantity' => 5,
                'sessionMode' => 'None',
            ],
        ];

        $result = ResolveSubscriptionResult::fromResponseData($responseData);

        self::assertSame($responseData['id'], $result->id);
        self::assertSame($responseData['subscriptionName'], $result->subscriptionName);
        self::assertSame($responseData['offerId'], $result->offerId);
        self::assertSame($responseData['planId'], $result->planId);
        self::assertEquals(Subscription::fromResponseData($responseData['subscription']), $result->subscription);
        self::assertSame($responseData, $result->rawData);
    }
}
