<?php

declare(strict_types=1);

namespace Keboola\ServiceClient\Tests;

use Keboola\ServiceClient\ServiceClient;
use Keboola\ServiceClient\ServiceDnsType;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class ServiceClientTest extends TestCase
{
    private const string PUBLIC_AI_SERVICE = 'https://ai.north-europe.azure.keboola.com';
    private const string PUBLIC_BILLING_SERVICE = 'https://billing.north-europe.azure.keboola.com';
    private const string PUBLIC_BUFFER_SERVICE = 'https://buffer.north-europe.azure.keboola.com';
    private const string PUBLIC_CONNECTION_SERVICE = 'https://connection.north-europe.azure.keboola.com';
    private const string PUBLIC_DATA_SCIENCE_SERVICE = 'https://data-science.north-europe.azure.keboola.com';
    private const string PUBLIC_ENCRYPTION_SERVICE = 'https://encryption.north-europe.azure.keboola.com';
    private const string PUBLIC_IMPORT_SERVICE = 'https://import.north-europe.azure.keboola.com';
    private const string PUBLIC_NOTIFICATION_SERVICE = 'https://notification.north-europe.azure.keboola.com';
    private const string PUBLIC_OAUTH = 'https://oauth.north-europe.azure.keboola.com';
    private const string PUBLIC_QUEUE = 'https://queue.north-europe.azure.keboola.com';
    private const string PUBLIC_SANDBOXES_SERVICE = 'https://sandboxes.north-europe.azure.keboola.com';
    private const string PUBLIC_SCHEDULER_SERVICE = 'https://scheduler.north-europe.azure.keboola.com';
    private const string PUBLIC_SYNC_ACTIONS_SERVICE = 'https://sync-actions.north-europe.azure.keboola.com';
    private const string PUBLIC_TEMPLATES = 'https://templates.north-europe.azure.keboola.com';
    private const string PUBLIC_VAULT = 'https://vault.north-europe.azure.keboola.com';

    private const string INTERNAL_AI_SERVICE = 'http://ai-service-api.default.svc.cluster.local';
    private const string INTERNAL_BILLING_SERVICE = 'http://billing-api.default.svc.cluster.local';
    private const string INTERNAL_BUFFER_SERVICE = 'http://buffer-api.buffer.svc.cluster.local';
    private const string INTERNAL_CONNECTION_SERVICE = 'http://connection-api.connection.svc.cluster.local';
    private const string INTERNAL_DATA_SCIENCE_SERVICE = 'http://sandboxes-service-api.default.svc.cluster.local';
    private const string INTERNAL_ENCRYPTION_SERVICE = 'http://encryption-api.default.svc.cluster.local';
    private const string INTERNAL_IMPORT_SERVICE = 'http://sapi-importer.default.svc.cluster.local';
    private const string INTERNAL_NOTIFICATION_SERVICE = 'http://notification-api.default.svc.cluster.local';
    private const string INTERNAL_OAUTH = 'http://oauth-api.default.svc.cluster.local';
    private const string INTERNAL_QUEUE = 'http://job-queue-api.default.svc.cluster.local';
    private const string INTERNAL_QUEUE_INTERNAL_API = 'http://job-queue-internal-api.default.svc.cluster.local';
    private const string INTERNAL_SANDBOXES_SERVICE = 'http://sandboxes-api.sandboxes.svc.cluster.local';
    private const string INTERNAL_SCHEDULER_SERVICE = 'http://scheduler-api.default.svc.cluster.local';
    private const string INTERNAL_SYNC_ACTIONS_SERVICE = 'http://runner-sync-api.default.svc.cluster.local';
    private const string INTERNAL_TEMPLATES = 'http://templates-api.templates-api.svc.cluster.local';
    private const string INTERNAL_VAULT = 'http://vault-api.default.svc.cluster.local';

    public function testGetExplicitPublicUrlMethods(): void
    {
        // configure for default INTERNAL dns and test that PUBLIC is properly passed from the method
        $client = new ServiceClient('north-europe.azure.keboola.com', ServiceDnsType::INTERNAL);

        self::assertSame(self::PUBLIC_AI_SERVICE, $client->getAiServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_BILLING_SERVICE, $client->getBillingServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_BUFFER_SERVICE, $client->getBufferServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_CONNECTION_SERVICE, $client->getConnectionServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_DATA_SCIENCE_SERVICE, $client->getSandboxesServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_ENCRYPTION_SERVICE, $client->getEncryptionServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_IMPORT_SERVICE, $client->getImportServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_NOTIFICATION_SERVICE, $client->getNotificationServiceUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_OAUTH, $client->getOauthUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_QUEUE, $client->getQueueUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_SANDBOXES_SERVICE, $client->getSandboxesApiUrl(ServiceDnsType::PUBLIC));
        self::assertSame(self::PUBLIC_SCHEDULER_SERVICE, $client->getSchedulerServiceUrl(ServiceDnsType::PUBLIC));
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
        self::assertSame(self::PUBLIC_DATA_SCIENCE_SERVICE, $client->getSandboxesServiceUrl());
        self::assertSame(self::PUBLIC_ENCRYPTION_SERVICE, $client->getEncryptionServiceUrl());
        self::assertSame(self::PUBLIC_IMPORT_SERVICE, $client->getImportServiceUrl());
        self::assertSame(self::PUBLIC_NOTIFICATION_SERVICE, $client->getNotificationServiceUrl());
        self::assertSame(self::PUBLIC_OAUTH, $client->getOauthUrl());
        self::assertSame(self::PUBLIC_QUEUE, $client->getQueueUrl());
        self::assertSame(self::PUBLIC_SANDBOXES_SERVICE, $client->getSandboxesApiUrl());
        self::assertSame(self::PUBLIC_SCHEDULER_SERVICE, $client->getSchedulerServiceUrl());
        self::assertSame(self::PUBLIC_SYNC_ACTIONS_SERVICE, $client->getSyncActionsServiceUrl());
        self::assertSame(self::PUBLIC_TEMPLATES, $client->getTemplatesUrl());
        self::assertSame(self::PUBLIC_VAULT, $client->getVaultUrl());
    }

    public function testGetExplicitPublicUrlOfServiceWithoutPublicDns(): void
    {
        // configure for default INTERNAL dns and test that PUBLIC is properly passed from the method
        $client = new ServiceClient('north-europe.azure.keboola.com', ServiceDnsType::INTERNAL);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Job queue internal API does not have public DNS');

        $client->getQueueInternalApiUrl(ServiceDnsType::PUBLIC);
    }

    public function testGetDefaultPublicUrlOfServiceWithoutPublicDns(): void
    {
        $client = new ServiceClient('north-europe.azure.keboola.com', ServiceDnsType::PUBLIC);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Job queue internal API does not have public DNS');

        $client->getQueueInternalApiUrl();
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
        self::assertSame(self::INTERNAL_DATA_SCIENCE_SERVICE, $client->getSandboxesServiceUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_ENCRYPTION_SERVICE, $client->getEncryptionServiceUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_IMPORT_SERVICE, $client->getImportServiceUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_NOTIFICATION_SERVICE, $client->getNotificationServiceUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_OAUTH, $client->getOauthUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_QUEUE, $client->getQueueUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_QUEUE_INTERNAL_API, $client->getQueueInternalApiUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_SANDBOXES_SERVICE, $client->getSandboxesApiUrl(ServiceDnsType::INTERNAL));
        self::assertSame(self::INTERNAL_SCHEDULER_SERVICE, $client->getSchedulerServiceUrl(ServiceDnsType::INTERNAL));
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
        self::assertSame(self::INTERNAL_DATA_SCIENCE_SERVICE, $client->getSandboxesServiceUrl());
        self::assertSame(self::INTERNAL_ENCRYPTION_SERVICE, $client->getEncryptionServiceUrl());
        self::assertSame(self::INTERNAL_IMPORT_SERVICE, $client->getImportServiceUrl());
        self::assertSame(self::INTERNAL_NOTIFICATION_SERVICE, $client->getNotificationServiceUrl());
        self::assertSame(self::INTERNAL_OAUTH, $client->getOauthUrl());
        self::assertSame(self::INTERNAL_QUEUE, $client->getQueueUrl());
        self::assertSame(self::INTERNAL_QUEUE_INTERNAL_API, $client->getQueueInternalApiUrl());
        self::assertSame(self::INTERNAL_SANDBOXES_SERVICE, $client->getSandboxesApiUrl());
        self::assertSame(self::INTERNAL_SCHEDULER_SERVICE, $client->getSchedulerServiceUrl());
        self::assertSame(self::INTERNAL_SYNC_ACTIONS_SERVICE, $client->getSyncActionsServiceUrl());
        self::assertSame(self::INTERNAL_TEMPLATES, $client->getTemplatesUrl());
        self::assertSame(self::INTERNAL_VAULT, $client->getVaultUrl());
    }
}
