<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\ServiceClient;

use Keboola\ApiBundle\Exception\ServiceInvalidException;
use Keboola\ApiBundle\Exception\ServiceNotFoundException;
use Keboola\ApiBundle\ServiceClient\ServiceClient;
use Keboola\ApiBundle\ServiceClient\ServiceDnsType;
use PHPUnit\Framework\TestCase;

class ServiceClientTest extends TestCase
{
    private const PUBLIC_AI_SERVICE = 'https://ai.north-europe.azure.keboola.com';
    private const PUBLIC_BILLING_SERVICE = 'https://billing.north-europe.azure.keboola.com';
    private const PUBLIC_BUFFER_SERVICE = 'https://buffer.north-europe.azure.keboola.com';
    private const PUBLIC_CONNECTION_SERVICE = 'https://connection.north-europe.azure.keboola.com';
    private const PUBLIC_DATA_SCIENCE_SERVICE = 'https://data-science.north-europe.azure.keboola.com';
    private const PUBLIC_ENCRYPTION_SERVICE = 'https://encryption.north-europe.azure.keboola.com';
    private const PUBLIC_IMPORT_SERVICE = 'https://import.north-europe.azure.keboola.com';
    private const PUBLIC_MLFLOW_SERVICE = 'https://mlflow.north-europe.azure.keboola.com';
    private const PUBLIC_NOTIFICATION_SERVICE = 'https://notification.north-europe.azure.keboola.com';
    private const PUBLIC_OAUTH = 'https://oauth.north-europe.azure.keboola.com';
    private const PUBLIC_QUEUE = 'https://queue.north-europe.azure.keboola.com';
    private const PUBLIC_SANDBOXES_SERVICE = 'https://sandboxes.north-europe.azure.keboola.com';
    private const PUBLIC_SCHEDULER_SERVICE = 'https://scheduler.north-europe.azure.keboola.com';
    private const PUBLIC_SPARK_SERVICE = 'https://spark.north-europe.azure.keboola.com';
    private const PUBLIC_SYNC_ACTIONS_SERVICE = 'https://sync-actions.north-europe.azure.keboola.com';
    private const PUBLIC_TEMPLATES = 'https://templates.north-europe.azure.keboola.com';
    private const PUBLIC_VAULT = 'https://vault.north-europe.azure.keboola.com';

    private const INTERNAL_AI_SERVICE = 'http://ai-service-api.default.svc.cluster.local';
    private const INTERNAL_BILLING_SERVICE = 'http://billing-api.buffer.svc.cluster.local';
    private const INTERNAL_BUFFER_SERVICE = 'http://buffer-api.default.svc.cluster.local';
    private const INTERNAL_CONNECTION_SERVICE = 'http://connection-api.connection.svc.cluster.local';
    private const INTERNAL_DATA_SCIENCE_SERVICE = 'http://sandboxes-service-api.default.svc.cluster.local';
    private const INTERNAL_ENCRYPTION_SERVICE = 'http://encryption-api.default.svc.cluster.local';
    private const INTERNAL_IMPORT_SERVICE = 'http://sapi-importer.default.svc.cluster.local';
    private const INTERNAL_MLFLOW_SERVICE = 'http://mlflow.default.svc.cluster.local';
    private const INTERNAL_NOTIFICATION_SERVICE = 'http://notification-api.default.svc.cluster.local';
    private const INTERNAL_OAUTH = 'http://oauth-api.default.svc.cluster.local';
    private const INTERNAL_QUEUE = 'http://job-queue-api.default.svc.cluster.local';
    private const INTERNAL_SANDBOXES_SERVICE = 'http://sandboxes-api.sandboxes.svc.cluster.local';
    private const INTERNAL_SCHEDULER_SERVICE = 'http://scheduler-api.default.svc.cluster.local';
    private const INTERNAL_SPARK_SERVICE = 'http://spark.default.svc.cluster.local';
    private const INTERNAL_SYNC_ACTIONS_SERVICE = 'http://runner-sync-api.default.svc.cluster.local';
    private const INTERNAL_TEMPLATES = 'http://templates-api.templates-api.svc.cluster.local';
    private const INTERNAL_VAULT = 'http://vault-api.default.svc.cluster.local';

    public function testGetExplicitPublicUrlMethods(): void
    {
        // configure for default INTERNAL dns and test that PUBLIC is properly passed from the method
        $client = new ServiceClient('north-europe.azure.keboola.com', ServiceDnsType::INTERNAL);

        self::assertSame(self::PUBLIC_AI_SERVICE, $client->getAiServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_BILLING_SERVICE, $client->getBillingServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_BUFFER_SERVICE, $client->getBufferServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_CONNECTION_SERVICE, $client->getConnectionServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_DATA_SCIENCE_SERVICE, $client->getDataScienceServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_ENCRYPTION_SERVICE, $client->getEncryptionServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_IMPORT_SERVICE, $client->getImportServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_MLFLOW_SERVICE, $client->getMlFlowServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_NOTIFICATION_SERVICE, $client->getNotificationServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_OAUTH, $client->getOauthUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_QUEUE, $client->getQueueUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_SANDBOXES_SERVICE, $client->getSandboxesServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_SCHEDULER_SERVICE, $client->getSchedulerServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_SPARK_SERVICE, $client->getSparkServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_SYNC_ACTIONS_SERVICE, $client->getSyncActionsServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_TEMPLATES, $client->getTemplatesUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_VAULT, $client->getVaultUrl(ServiceDnsType::PUBLIC));
    }

    public function testGetDefaultPublicUrlMethods(): void
    {
        $client = new ServiceClient('north-europe.azure.keboola.com', ServiceDnsType::PUBLIC);

        self::assertSame(self::PUBLIC_AI_SERVICE, $client->getAiServiceUrl());
        self::assertSame(self::PUBLIC_BILLING_SERVICE, $client->getBillingServiceUrl());
        self::assertSame(self::PUBLIC_BUFFER_SERVICE, $client->getBufferServiceUrl());
        self::assertSame(self::PUBLIC_CONNECTION_SERVICE, $client->getConnectionServiceUrl());
        self::assertSame(self::PUBLIC_DATA_SCIENCE_SERVICE, $client->getDataScienceServiceUrl());
        self::assertSame(self::PUBLIC_ENCRYPTION_SERVICE, $client->getEncryptionServiceUrl());
        self::assertSame(self::PUBLIC_IMPORT_SERVICE, $client->getImportServiceUrl());
        self::assertSame(self::PUBLIC_MLFLOW_SERVICE, $client->getMlFlowServiceUrl());
        self::assertSame(self::PUBLIC_NOTIFICATION_SERVICE, $client->getNotificationServiceUrl());
        self::assertSame(self::PUBLIC_OAUTH, $client->getOauthUrl());
        self::assertSame(self::PUBLIC_QUEUE, $client->getQueueUrl());
        self::assertSame(self::PUBLIC_SANDBOXES_SERVICE, $client->getSandboxesServiceUrl());
        self::assertSame(self::PUBLIC_SCHEDULER_SERVICE, $client->getSchedulerServiceUrl());
        self::assertSame(self::PUBLIC_SPARK_SERVICE, $client->getSparkServiceUrl());
        self::assertSame(self::PUBLIC_SYNC_ACTIONS_SERVICE, $client->getSyncActionsServiceUrl());
        self::assertSame(self::PUBLIC_TEMPLATES, $client->getTemplatesUrl());
        self::assertSame(self::PUBLIC_VAULT, $client->getVaultUrl());
    }

    public function testGetExplicitInternalUrlMethods(): void
    {
        // configure for default PUBLIC dns and test that INTERNAL is properly passed from the method
        $client = new ServiceClient('north-europe.azure.keboola.com', ServiceDnsType::PUBLIC);

        // phpcs:disable Generic.Files.LineLength
        self::assertSame(self::INTERNAL_AI_SERVICE, $client->getAiServiceUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_BILLING_SERVICE, $client->getBillingServiceUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_BUFFER_SERVICE, $client->getBufferServiceUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_CONNECTION_SERVICE, $client->getConnectionServiceUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_DATA_SCIENCE_SERVICE, $client->getDataScienceServiceUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_ENCRYPTION_SERVICE, $client->getEncryptionServiceUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_IMPORT_SERVICE, $client->getImportServiceUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_MLFLOW_SERVICE, $client->getMlFlowServiceUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_NOTIFICATION_SERVICE, $client->getNotificationServiceUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_OAUTH, $client->getOauthUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_QUEUE, $client->getQueueUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_SANDBOXES_SERVICE, $client->getSandboxesServiceUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_SCHEDULER_SERVICE, $client->getSchedulerServiceUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_SPARK_SERVICE, $client->getSparkServiceUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_SYNC_ACTIONS_SERVICE, $client->getSyncActionsServiceUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_TEMPLATES, $client->getTemplatesUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_VAULT, $client->getVaultUrl(ServiceDnsType::INTERNAL));
        // phpcs:enable Generic.Files.LineLength
    }

    public function testGetDefaultInternalUrlMethods(): void
    {
        $client = new ServiceClient('north-europe.azure.keboola.com', ServiceDnsType::INTERNAL);

        self::assertSame(self::INTERNAL_AI_SERVICE, $client->getAiServiceUrl());
        self::assertSame(self::INTERNAL_BILLING_SERVICE, $client->getBillingServiceUrl());
        self::assertSame(self::INTERNAL_BUFFER_SERVICE, $client->getBufferServiceUrl());
        self::assertSame(self::INTERNAL_CONNECTION_SERVICE, $client->getConnectionServiceUrl());
        self::assertSame(self::INTERNAL_DATA_SCIENCE_SERVICE, $client->getDataScienceServiceUrl());
        self::assertSame(self::INTERNAL_ENCRYPTION_SERVICE, $client->getEncryptionServiceUrl());
        self::assertSame(self::INTERNAL_IMPORT_SERVICE, $client->getImportServiceUrl());
        self::assertSame(self::INTERNAL_MLFLOW_SERVICE, $client->getMlFlowServiceUrl());
        self::assertSame(self::INTERNAL_NOTIFICATION_SERVICE, $client->getNotificationServiceUrl());
        self::assertSame(self::INTERNAL_OAUTH, $client->getOauthUrl());
        self::assertSame(self::INTERNAL_QUEUE, $client->getQueueUrl());
        self::assertSame(self::INTERNAL_SANDBOXES_SERVICE, $client->getSandboxesServiceUrl());
        self::assertSame(self::INTERNAL_SCHEDULER_SERVICE, $client->getSchedulerServiceUrl());
        self::assertSame(self::INTERNAL_SPARK_SERVICE, $client->getSparkServiceUrl());
        self::assertSame(self::INTERNAL_SYNC_ACTIONS_SERVICE, $client->getSyncActionsServiceUrl());
        self::assertSame(self::INTERNAL_TEMPLATES, $client->getTemplatesUrl());
        self::assertSame(self::INTERNAL_VAULT, $client->getVaultUrl());
    }
}
