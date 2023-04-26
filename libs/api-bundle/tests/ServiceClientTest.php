<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests;

use Keboola\ApiBundle\ServiceClient;
use Keboola\ApiBundle\Exception\ServiceInvalidException;
use Keboola\ApiBundle\Exception\ServiceNotFoundException;
use PHPUnit\Framework\TestCase;

class ServiceClientTest extends TestCase
{
    public function testGetUrlMethods(): void
    {
        $client = new ServiceClient('north-europe.azure.keboola.com');
        self::assertSame('https://ai.north-europe.azure.keboola.com', $client->getAiServiceUrl());
        self::assertSame('https://billing.north-europe.azure.keboola.com', $client->getBillingServiceUrl());
        self::assertSame('https://buffer.north-europe.azure.keboola.com', $client->getBufferServiceUrl());
        self::assertSame('https://connection.north-europe.azure.keboola.com', $client->getConnectionServiceUrl());
        self::assertSame('https://data-science.north-europe.azure.keboola.com', $client->getDataScienceServiceUrl());
        self::assertSame('https://encryption.north-europe.azure.keboola.com', $client->getEncryptionServiceUrl());
        self::assertSame('https://import.north-europe.azure.keboola.com', $client->getImportServiceUrl());
        self::assertSame('https://mlflow.north-europe.azure.keboola.com', $client->getMlFlowServiceUrl());
        self::assertSame('https://notification.north-europe.azure.keboola.com', $client->getNotificationServiceUrl());
        self::assertSame('https://oauth.north-europe.azure.keboola.com', $client->getOauthUrl());
        self::assertSame('https://queue.north-europe.azure.keboola.com', $client->getQueueUrl());
        self::assertSame('https://sandboxes.north-europe.azure.keboola.com', $client->getSandboxesServiceUrl());
        self::assertSame('https://scheduler.north-europe.azure.keboola.com', $client->getSchedulerServiceUrl());
        self::assertSame('https://spark.north-europe.azure.keboola.com', $client->getSparkServiceUrl());
        self::assertSame('https://sync-actions.north-europe.azure.keboola.com', $client->getSyncActionsServiceUrl());
        self::assertSame('https://templates.north-europe.azure.keboola.com', $client->getTemplatesUrl());
    }

    public function testInvalidService(): void
    {
        $this->expectException(ServiceInvalidException::class);
        $this->expectExceptionMessage('Service "non-existent" is not known.');
        $client = new ServiceClient('keboola.com');
        $client->getServiceUrl('non-existent');
    }

    public function testNonExistentService(): void
    {
        $this->expectException(ServiceNotFoundException::class);
        $this->expectExceptionMessage('Billing service is not available on stack "keboola.com".');
        $client = new ServiceClient('keboola.com');
        $client->getServiceUrl(ServiceClient::BILLING_SERVICE);
    }
}
