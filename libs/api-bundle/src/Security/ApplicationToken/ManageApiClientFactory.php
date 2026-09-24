<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\ApplicationToken;

use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\ServiceClient\ServiceClient;
use Keboola\ServiceClient\ServiceDnsType;

class ManageApiClientFactory
{
    /**
     * @param ?int $backoffMaxTries Retry cap for every client this factory builds. Null leaves
     *     {@see ManageApiClient}'s own default of 10 (1023 s of blocking sleep) in place.
     */
    public function __construct(
        private readonly string $appName,
        private readonly ServiceClient $serviceClient,
        private readonly ?int $backoffMaxTries = null,
    ) {
    }

    /**
     * @return array<string, mixed>
     */
    private function baseConfig(?ServiceDnsType $dnsType = null): array
    {
        $config = [
            'url' => $this->serviceClient->getConnectionServiceUrl($dnsType),
            'userAgent' => $this->appName,
        ];
        if ($this->backoffMaxTries !== null) {
            $config['backoffMaxTries'] = $this->backoffMaxTries;
        }

        return $config;
    }

    public function getClientForManageToken(string $token): ManageApiClient
    {
        return new ManageApiClient($this->baseConfig() + ['token' => $token]);
    }

    public function getClientForServiceAccountToken(string $jwt): ManageApiClient
    {
        return new ManageApiClient($this->baseConfig() + ['jwtToken' => $jwt]);
    }

    /**
     * Builds a client that authenticates with the service's projected Kubernetes ServiceAccount
     * JWT read from $tokenPath. The token file is re-read by the client on every request, so
     * kubelet-rotated tokens are picked up automatically. Used for service-to-service calls
     * (e.g. the auth-bridge storage token resolver). DNS type defaults to the ServiceClient's
     * configured default, same as the other factory methods.
     */
    public function getClientForServiceAccountTokenPath(
        string $tokenPath,
        ?ServiceDnsType $dnsType = null,
    ): ManageApiClient {
        return new ManageApiClient($this->baseConfig($dnsType) + ['kubernetesTokenPath' => $tokenPath]);
    }
}
