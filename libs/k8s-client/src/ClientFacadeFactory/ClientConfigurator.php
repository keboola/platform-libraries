<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFacadeFactory;

use Keboola\K8sClient\ApiClient\StreamClient;
use KubernetesRuntime\Client;

class ClientConfigurator
{
    public static function configureClients(
        string $apiUrl,
        string $caCertFile,
        string $token,
    ): void {
        $authenticationInfo = [
            'caCert' => $caCertFile,
            'token' => $token,
        ];

        $guzzleOptions = [
            'connect_timeout' => '30',
            'timeout' => '60',
        ];

        Client::configure($apiUrl, $authenticationInfo, $guzzleOptions);
        StreamClient::configure($apiUrl, $authenticationInfo, $guzzleOptions);
    }
}
