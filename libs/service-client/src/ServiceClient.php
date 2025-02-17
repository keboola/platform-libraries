<?php

declare(strict_types=1);

namespace Keboola\ServiceClient;

use InvalidArgumentException;
use RuntimeException;
use ValueError;

class ServiceClient
{
    private const K8S_SUFFIX = 'svc.cluster.local';

    private readonly ServiceDnsType $defaultDnsType;

    /**
     * @param non-empty-string|null $hostnameSuffix
     */
    public function __construct(
        private readonly ?string $hostnameSuffix,
        ServiceDnsType|string $defaultDnsType = ServiceDnsType::PUBLIC,
    ) {
        if (is_string($defaultDnsType)) {
            try {
                $defaultDnsType = ServiceDnsType::from($defaultDnsType);
            } catch (ValueError $e) {
                throw new InvalidArgumentException(
                    sprintf('"%s" is not valid service DNS type', $defaultDnsType),
                    $e->getCode(),
                    $e,
                );
            }
        }

        if ($this->hostnameSuffix === null && $defaultDnsType === ServiceDnsType::PUBLIC) {
            throw new InvalidArgumentException('Hostname suffix must be provided when using public DNS type.');
        }

        $this->defaultDnsType = $defaultDnsType;
    }

    /**
     * Creates ServiceClient with hostnameSuffix configured same as existing service URL. Always configures
     * ServiceContainer to use PUBLIC Dns type.
     */
    public static function fromServicePublicUrl(Service $service, string $url): self
    {
        $host = parse_url($url, PHP_URL_HOST);
        if ($host === false || $host === null || $host === '') {
            throw new InvalidArgumentException(sprintf('Invalid URL "%s"', $url));
        }

        $serviceSubdomain = $service->getPublicSubdomain();
        $hostPrefix = $serviceSubdomain . '.';

        if (!str_starts_with($host, $serviceSubdomain)) {
            throw new InvalidArgumentException(sprintf(
                '"%s" is not %s service URL',
                $url,
                $service->name,
            ));
        }

        $hostSuffix = substr($host, strlen($hostPrefix));

        // this is always true, because $hostPrefix always ends with dot, and $host never ends with dot
        // (otherwise parse_url would fail)
        assert(strlen($hostSuffix) > 0);

        return new self($hostSuffix, ServiceDnsType::PUBLIC);
    }

    /**
     * @return non-empty-string
     */
    public function getServiceUrl(Service $service, ?ServiceDnsType $dnsType = null): string
    {
        $dnsType ??= $this->defaultDnsType;

        if ($dnsType === ServiceDnsType::INTERNAL) {
            return sprintf('http://%s.%s', $service->getInternalServiceName(), self::K8S_SUFFIX);
        }

        if ($this->hostnameSuffix === null) {
            throw new RuntimeException('Can\'t get URL for public DNS type, hostname suffix was not configured.');
        }

        return sprintf('https://%s.%s', $service->getPublicSubdomain(), $this->hostnameSuffix);
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
}
