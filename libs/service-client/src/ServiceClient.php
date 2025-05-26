<?php

declare(strict_types=1);

namespace Keboola\ServiceClient;

class ServiceClient
{
    private const K8S_SUFFIX = 'svc.cluster.local';

    private readonly ServiceDnsType $defaultDnsType;

    /**
     * @param non-empty-string $hostnameSuffix
     */
    public function __construct(
        private readonly string $hostnameSuffix,
        ServiceDnsType|string $defaultDnsType = ServiceDnsType::PUBLIC,
    ) {
        if (is_string($defaultDnsType)) {
            $defaultDnsType = ServiceDnsType::from($defaultDnsType);
        }

        $this->defaultDnsType = $defaultDnsType;
    }

    /**
     * @return non-empty-string
     */
    public function getServiceUrl(Service $service, ?ServiceDnsType $dnsType = null): string
    {
        return match ($dnsType ?? $this->defaultDnsType) {
            ServiceDnsType::INTERNAL => sprintf('http://%s.%s', $service->getInternalServiceName(), self::K8S_SUFFIX),
            ServiceDnsType::PUBLIC => sprintf('https://%s.%s', $service->getPublicSubdomain(), $this->hostnameSuffix),
        };
    }

    /**
     * @return non-empty-string
     */
    public function getAiServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(Service::AI, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getBillingServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(Service::BILLING, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getBufferServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(Service::BUFFER, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getConnectionServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(Service::CONNECTION, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getSandboxesServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(Service::SANDBOXES_SERVICE, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getEncryptionServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(Service::ENCRYPTION, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getImportServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(Service::IMPORT, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getOauthUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(Service::OAUTH, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getNotificationServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(Service::NOTIFICATION, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getSandboxesApiUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(Service::SANDBOXES_API, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getSchedulerServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(Service::SCHEDULER, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getSyncActionsServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(Service::SYNC_ACTIONS, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getQueueUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(Service::QUEUE, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getQueueInternalApiUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(Service::QUEUE_INTERNAL_API, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getTemplatesUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(Service::TEMPLATES, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getVaultUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(Service::VAULT, $dnsType);
    }

    /**
     * @return non-empty-string
     */
    public function getWaiiServiceUrl(?ServiceDnsType $dnsType = null): string
    {
        return $this->getServiceUrl(Service::WAII, $dnsType);
    }
}
