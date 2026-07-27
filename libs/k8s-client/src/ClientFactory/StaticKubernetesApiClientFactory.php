<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFactory;

use InvalidArgumentException;
use Keboola\K8sClient\ClientFactory\Token\TokenInterface;
use Keboola\K8sClient\KubernetesApiClient;
use Retry\RetryProxy;

class StaticKubernetesApiClientFactory implements KubernetesApiClientFactory
{
    public function __construct(
        private readonly RetryProxy $retryProxy,
        private readonly string $apiUrl,
        private readonly TokenInterface|string $token,
        private readonly string $caCertFile,
        private readonly ?string $defaultNamespace = null,
    ) {
    }

    public function createApiClient(?string $namespace = null): KubernetesApiClient
    {
        $namespace ??= $this->defaultNamespace;

        if ($namespace === null) {
            throw new InvalidArgumentException(
                'Namespace must be provided either as an argument or configured as the default namespace.',
            );
        }

        ClientConfigurator::configureBaseClient($this->apiUrl, $this->caCertFile, $this->token);

        return new KubernetesApiClient($this->retryProxy, $namespace);
    }
}
