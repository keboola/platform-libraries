<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\EventGrid;

use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\Authenticator\SASTokenAuthenticatorFactory;
use Keboola\AzureApiClient\Json;
use Keboola\AzureApiClient\ServiceBus\Model\ServiceBusBrokerMessageRequest;
use Keboola\AzureApiClient\ServiceBus\ServiceBusApiClient;
use Keboola\AzureApiClient\Tests\ReflectionPropertyAccessTestCase;
use PHPUnit\Framework\TestCase;
use function _PHPStan_a3459023a\RingCentral\Psr7\str;

/**
 * @group functional
 */
class ServiceBusApiClientFunctionalTest extends TestCase
{
    use ReflectionPropertyAccessTestCase;

    private function getClient(): ServiceBusApiClient
    {
        $endpoint = (string) getenv('AZURE_API_CLIENT_CI__SERVICE_BUS__ENDPOINT');
        return new ServiceBusApiClient(
            //@phpstan-ignore-next-line
            serviceBusEndpoint: $endpoint,
            configuration: new ApiClientConfiguration(
                authenticator: new SASTokenAuthenticatorFactory(
                    url: $endpoint,
                    sharedAccessKeyName: 'RootManageSharedAccessKey',
                    sharedAccessKey: (string) getenv('AZURE_API_CLIENT_CI__SERVICE_BUS__SHARED_ACCESS_KEY'),
                ),
            )
        );
    }

    public function testEvent(): void
    {
        $client = $this->getClient();
        $messageSend = ServiceBusBrokerMessageRequest::createJson(
            '123',
            ['testdata' => 'value']
        );
        $client->sendMessage(
            'queue-tests',
            $messageSend
        );
        $messageReceived = $client->peakMessage(
            'queue-tests'
        );
        $this->assertNotNull($messageReceived);
        self::assertSame($messageSend->id, $messageReceived->id);
        $messageBody = Json::decodeArray($messageReceived->body);
        self::assertSame([
            'testdata' => 'value',
        ], $messageBody);
        $client->deleteMessage($messageReceived->lockLocation);
    }
}
