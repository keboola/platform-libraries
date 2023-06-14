<?php

declare(strict_types=1);

namespace Keboola\ApiBundle;

use Keboola\ApiBundle\Exception\ServiceInvalidException;
use Keboola\ApiBundle\Exception\ServiceNotFoundException;

class ServiceClient
{
    public const AI_SERVICE = 'ai';
    public const BILLING_SERVICE = 'billing';
    public const BUFFER_SERVICE = 'buffer';
    public const CONNECTION_SERVICE = 'connection';
    public const DATA_SCIENCE_SERVICE = 'data-science';
    public const ENCRYPTION_SERVICE = 'encryption';
    public const IMPORT_SERVICE = 'import';
    public const OAUTH_SERVICE = 'oauth';
    public const MLFLOW_SERVICE = 'mlflow';
    public const NOTIFICATION_SERVICE = 'notification';
    public const SANDBOXES_SERVICE = 'sandboxes';
    public const SCHEDULER_SERVICE = 'scheduler';
    public const SPARK_SERVICE = 'spark';
    public const SYNC_ACTIONS_SERVICE = 'sync-actions';
    public const QUEUE_SERVICE = 'queue';
    public const TEMPLATES_SERVICE = 'templates';
    private const KNOWN_SERVICES = [
        self::AI_SERVICE,
        self::BILLING_SERVICE,
        self::BUFFER_SERVICE,
        self::CONNECTION_SERVICE,
        self::DATA_SCIENCE_SERVICE,
        self::ENCRYPTION_SERVICE,
        self::IMPORT_SERVICE,
        self::OAUTH_SERVICE,
        self::MLFLOW_SERVICE,
        self::NOTIFICATION_SERVICE,
        self::SANDBOXES_SERVICE,
        self::SCHEDULER_SERVICE,
        self::SPARK_SERVICE,
        self::SYNC_ACTIONS_SERVICE,
        self::QUEUE_SERVICE,
        self::TEMPLATES_SERVICE,
    ];

    public function __construct(private readonly string $hostnameSuffix)
    {
    }

    public function getServiceUrl(string $serviceName): string
    {
        if (($this->hostnameSuffix !== 'north-europe.azure.keboola.com') && ($serviceName === self::BILLING_SERVICE)) {
            throw new ServiceNotFoundException(
                sprintf('Billing service is not available on stack "%s".', $this->hostnameSuffix)
            );
        }
        if (in_array($serviceName, self::KNOWN_SERVICES, true)) {
            return sprintf('https://%s.%s', $serviceName, $this->hostnameSuffix);
        }
        throw new ServiceInvalidException(sprintf('Service "%s" is not known.', $serviceName));
    }

    public function getAiServiceUrl(): string
    {
        return $this->getServiceUrl(self::AI_SERVICE);
    }

    public function getBillingServiceUrl(): string
    {
        return $this->getServiceUrl(self::BILLING_SERVICE);
    }

    public function getBufferServiceUrl(): string
    {
        return $this->getServiceUrl(self::BUFFER_SERVICE);
    }

    public function getConnectionServiceUrl(): string
    {
        return $this->getServiceUrl(self::CONNECTION_SERVICE);
    }

    public function getDataScienceServiceUrl(): string
    {
        return $this->getServiceUrl(self::DATA_SCIENCE_SERVICE);
    }

    public function getEncryptionServiceUrl(): string
    {
        return $this->getServiceUrl(self::ENCRYPTION_SERVICE);
    }

    public function getImportServiceUrl(): string
    {
        return $this->getServiceUrl(self::IMPORT_SERVICE);
    }

    public function getOauthUrl(): string
    {
        return $this->getServiceUrl(self::OAUTH_SERVICE);
    }

    public function getMlFlowServiceUrl(): string
    {
        return $this->getServiceUrl(self::MLFLOW_SERVICE);
    }

    public function getNotificationServiceUrl(): string
    {
        return $this->getServiceUrl(self::NOTIFICATION_SERVICE);
    }

    public function getSandboxesServiceUrl(): string
    {
        return $this->getServiceUrl(self::SANDBOXES_SERVICE);
    }

    public function getSchedulerServiceUrl(): string
    {
        return $this->getServiceUrl(self::SCHEDULER_SERVICE);
    }

    public function getSparkServiceUrl(): string
    {
        return $this->getServiceUrl(self::SPARK_SERVICE);
    }

    public function getSyncActionsServiceUrl(): string
    {
        return $this->getServiceUrl(self::SYNC_ACTIONS_SERVICE);
    }

    public function getQueueUrl(): string
    {
        return $this->getServiceUrl(self::QUEUE_SERVICE);
    }

    public function getTemplatesUrl(): string
    {
        return $this->getServiceUrl(self::TEMPLATES_SERVICE);
    }
}
