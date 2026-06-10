<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\ApplicationToken;

use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\ServiceClient\ServiceClient;
use Keboola\ServiceClient\ServiceDnsType;

class ManageApiClientFactory
{
    public function __construct(
        private readonly string $appName,
        private readonly ServiceClient $serviceClient,
    ) {
    }

    public function getClientForManageToken(string $token): ManageApiClient
    {
        return new ManageApiClient([
            'url' => $this->serviceClient->getConnectionServiceUrl(),
            'token' => $token,
            'userAgent' => $this->appName,
        ]);
    }

    public function getClientForServiceAccountToken(string $jwt): ManageApiClient
    {
        return new ManageApiClient([
            'url' => $this->serviceClient->getConnectionServiceUrl(),
            'jwtToken' => $jwt,
            'userAgent' => $this->appName,
        ]);
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
        return new ManageApiClient([
            'url' => $this->serviceClient->getConnectionServiceUrl($dnsType),
            'kubernetesTokenPath' => $tokenPath,
            'userAgent' => $this->appName,
        ]);
    }
}
