<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests;

use Keboola\K8sClient\ApiClient\EventsApiClient;
use Keboola\K8sClient\ApiClient\PodsApiClient;
use Keboola\K8sClient\ApiClient\SecretsApiClient;
use Keboola\K8sClient\ClientFacadeFactory\GenericClientFacadeFactory;
use Keboola\K8sClient\Exception\ResourceNotFoundException;
use Keboola\K8sClient\Exception\TimeoutException;
use Keboola\K8sClient\KubernetesApiClientFacade;
use Keboola\K8sClient\RetryProxyFactory;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Event;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Pod;
use Kubernetes\Model\Io\K8s\Api\Core\V1\PodList;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Secret;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use RuntimeException;

class KubernetesApiClientFacadeFunctionalTest extends TestCase
{
    private KubernetesApiClientFacade $apiClient;

    public function setUp(): void
    {
        parent::setUp();

        $logger = new Logger('test');

        $this->apiClient = (new GenericClientFacadeFactory(
            (new RetryProxyFactory($logger))->createRetryProxy(),
            $logger
        ))->createClusterClient(
            (string) getenv('K8S_HOST'),
            (string) getenv('K8S_TOKEN'),
            (string) getenv('K8S_CA_CERT_PATH'),
            (string) getenv('K8S_NAMESPACE'),
        );

        $this->cleanupCluster();
    }

    public function tearDown(): void
    {
        parent::tearDown();
        $this->cleanupCluster();
    }

    private function cleanupCluster(): void
    {
        $this->apiClient->deleteAllMatching(
            new DeleteOptions(['gracePeriodSeconds' => 0]),
            ['labelSelector' => 'app=KubernetesApiClientFacadeFunctionalTest']
        );
    }

    public function testListMatching(): void
    {
        foreach ([
            ['name' => 'pod-1', 'label' => 'group-1'],
            ['name' => 'pod-2', 'label' => 'group-2'],
            ['name' => 'pod-3', 'label' => 'group-2'],
            ['name' => 'pod-4', 'label' => 'group-2'],
            ['name' => 'pod-5', 'label' => 'group-3'],
        ] as $podData) {
            $this->apiClient->pods()->create(new Pod([
                'metadata' => [
                    'name' => $podData['name'],
                    'labels' => [
                        'app' => 'KubernetesApiClientFacadeFunctionalTest',
                        'group' => $podData['label'],
                    ],
                ],
                'spec' => [
                    'containers' => [
                        [
                            'name' => 'nginx',
                            'image' => 'nginx',
                        ],
                    ],
                ],
            ]));
        }

        $group1 = [...$this->apiClient->listMatching(Pod::class, ['labelSelector' => 'group=group-1'])];
        self::assertCount(1, $group1);
        self::assertSame('pod-1', $group1[0]->metadata->name);

        $group2 = [...$this->apiClient->listMatching(Pod::class, ['labelSelector' => 'group=group-2'])];
        self::assertCount(3, $group2);
        self::assertSame('pod-2', $group2[0]->metadata->name);
        self::assertSame('pod-3', $group2[1]->metadata->name);
        self::assertSame('pod-4', $group2[2]->metadata->name);

        $allPods = [...$this->apiClient->listMatching(Pod::class)];
        self::assertCount(5, $allPods);
        self::assertSame('pod-1', $allPods[0]->metadata->name);
        self::assertSame('pod-2', $allPods[1]->metadata->name);
        self::assertSame('pod-3', $allPods[2]->metadata->name);
        self::assertSame('pod-4', $allPods[3]->metadata->name);
        self::assertSame('pod-5', $allPods[4]->metadata->name);

        $noPods = [...$this->apiClient->listMatching(Pod::class, ['labelSelector' => 'foo=bar'])];
        self::assertCount(0, $noPods);
    }
}
