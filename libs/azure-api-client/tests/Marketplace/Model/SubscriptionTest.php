<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Marketplace\Model;

use Keboola\AzureApiClient\Marketplace\Model\Subscription;
use PHPUnit\Framework\TestCase;

class SubscriptionTest extends TestCase
{
    public function testFromResponseData(): void
    {
        // example data from https://learn.microsoft.com/en-us/azure/marketplace/partner-center-portal/pc-saas-fulfillment-subscription-api#post-httpsmarketplaceapimicrosoftcomapisaassubscriptionsresolveapi-versionapiversion
        $responseData = [
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
        ];

        $result = Subscription::fromResponseData($responseData);

        self::assertSame('<guid>', $result->id);
        self::assertSame('contoso', $result->publisherId);
        self::assertSame('offer1', $result->offerId);
        self::assertSame('silver', $result->planId);
        self::assertSame('Contoso Cloud Solution', $result->name);
        self::assertSame(5, $result->quantity);
        self::assertSame(' PendingFulfillmentStart ', $result->saasSubscriptionStatus);
    }
}
