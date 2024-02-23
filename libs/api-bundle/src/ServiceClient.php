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
    public const VAULT_SERVICE = 'vault';

    private const NAMESPACE_DEFAULT = 'default';
    private const NAMESPACE_BUFFER = 'buffer';
    private const NAMESPACE_CONNECTION = 'connection';
    private const NAMESPACE_SANDBOXES = 'sandboxes';
    private const NAMESPACE_TEMPLATES = 'templates-api';

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
        self::VAULT_SERVICE,
    ];

    private const SERVICES_NAMESPACES = [
        self::AI_SERVICE => self::NAMESPACE_DEFAULT,
        self::BILLING_SERVICE => self::NAMESPACE_DEFAULT,
        self::BUFFER_SERVICE => self::NAMESPACE_BUFFER,
        self::CONNECTION_SERVICE => self::NAMESPACE_CONNECTION,
        self::DATA_SCIENCE_SERVICE => self::NAMESPACE_DEFAULT,
        self::ENCRYPTION_SERVICE => self::NAMESPACE_DEFAULT,
        self::IMPORT_SERVICE => self::NAMESPACE_DEFAULT,
        self::OAUTH_SERVICE => self::NAMESPACE_DEFAULT,
        self::MLFLOW_SERVICE => self::NAMESPACE_DEFAULT, // ??
        self::NOTIFICATION_SERVICE => self::NAMESPACE_DEFAULT,
        self::SANDBOXES_SERVICE => self::NAMESPACE_SANDBOXES,
        self::SCHEDULER_SERVICE => self::NAMESPACE_DEFAULT,
        self::SPARK_SERVICE => self::NAMESPACE_DEFAULT, // ??
        self::SYNC_ACTIONS_SERVICE => self::NAMESPACE_DEFAULT,
        self::QUEUE_SERVICE => self::NAMESPACE_DEFAULT,
        self::TEMPLATES_SERVICE => self::NAMESPACE_TEMPLATES,
        self::VAULT_SERVICE => self::NAMESPACE_DEFAULT,
    ];

    /**
     * @param non-empty-string $hostnameSuffix
     */
    public function __construct(
        private readonly string $hostnameSuffix,
        private readonly ServiceDnsType $dnsType = ServiceDnsType::PUBLIC,
    ) {
    }

    /**
     * @param value-of<self::KNOWN_SERVICES> $serviceName
     * @return non-empty-string
     */
    public function getServiceUrl(string $serviceName, ?ServiceDnsType $dnsType = null): string
    {
        if (!in_array($serviceName, self::KNOWN_SERVICES, true)) {
            throw new ServiceInvalidException(sprintf('Service "%s" is not known.', $serviceName));
        }

        return match ($dnsType ?? $this->dnsType) {
            ServiceDnsType::INTERNAL => sprintf(
                'http://%s.%s.svc.cluster.local',
                ($serviceName === self::DATA_SCIENCE_SERVICE ? 'sandboxes-service' : $serviceName),
                self::SERVICES_NAMESPACES[$serviceName],
            ),

            ServiceDnsType::PUBLIC => sprintf(
                'https://%s.%s',
                $serviceName,
                $this->hostnameSuffix,
            ),
        };
    }

    /**
     * @return non-empty-string
     */
    public function getAiServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(self::AI_SERVICE, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getBillingServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(self::BILLING_SERVICE, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getBufferServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(self::BUFFER_SERVICE, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getConnectionServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(self::CONNECTION_SERVICE, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getDataScienceServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(self::DATA_SCIENCE_SERVICE, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getEncryptionServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(self::ENCRYPTION_SERVICE, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getImportServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(self::IMPORT_SERVICE, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getOauthUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(self::OAUTH_SERVICE, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getMlFlowServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(self::MLFLOW_SERVICE, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getNotificationServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(self::NOTIFICATION_SERVICE, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getSandboxesServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(self::SANDBOXES_SERVICE, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getSchedulerServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(self::SCHEDULER_SERVICE, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getSparkServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(self::SPARK_SERVICE, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getSyncActionsServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(self::SYNC_ACTIONS_SERVICE, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getQueueUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(self::QUEUE_SERVICE, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getTemplatesUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(self::TEMPLATES_SERVICE, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getVaultUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(self::VAULT_SERVICE, $dnsType);
    }
}
