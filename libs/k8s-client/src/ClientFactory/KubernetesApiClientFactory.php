<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFactory;

use Keboola\K8sClient\KubernetesApiClient;

interface KubernetesApiClientFactory
{
    public function createApiClient(?string $namespace = null): KubernetesApiClient;
}
