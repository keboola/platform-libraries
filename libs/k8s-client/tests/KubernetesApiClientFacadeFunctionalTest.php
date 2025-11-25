<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests;

use Keboola\K8sClient\ClientFacadeFactory\GenericClientFacadeFactory;
use Keboola\K8sClient\KubernetesApiClientFacade;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\App;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppRun;
use Keboola\K8sClient\RetryProxyFactory;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Pod;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

class KubernetesApiClientFacadeFunctionalTest extends TestCase
{
    private KubernetesApiClientFacade $apiClient;

    public function setUp(): void
    {
        parent::setUp();

        $logger = new Logger('test');

        $this->apiClient = (new GenericClientFacadeFactory(
            (new RetryProxyFactory($logger))->createRetryProxy(),
            $logger,
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
            [
                'fieldSelector' => implode(
                    ',',
                    array_map(
                        fn (string $name) => sprintf('metadata.name!=%s', $name),
                        [
                            'kube-root-ca.crt', // secret automatically created by K8S
                            'k8s-client', // service account used by the client
                        ],
                    ),
                ),
            ],
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

    public function testMergePatch(): void
    {
        // Create initial app
        $app = new App([
            'metadata' => [
                'name' => 'test-patch-app',
                'labels' => ['app' => 'KubernetesApiClientFacadeFunctionalTest'],
            ],
            'spec' => [
                'appId' => 'app-123',
                'projectId' => 'project-456',
                'state' => 'Running',
                'replicas' => 1,
                'podSpec' => [
                    'restartPolicy' => 'Always',
                    'containers' => [[
                        'name' => 'main',
                        'image' => 'busybox',
                    ]],
                ],
            ],
        ]);
        $created = $this->apiClient->apps()->create($app);

        // Verify initial state
        self::assertNotNull($created->spec);
        self::assertSame('Running', $created->spec->state);
        self::assertSame(1, $created->spec->replicas);

        // Update via patch
        self::assertNotNull($app->spec);
        $app->spec->replicas = 3;
        $app->spec->state = 'Stopped';

        $patched = $this->apiClient->mergePatch($app);

        self::assertNotNull($patched->metadata);
        self::assertSame('test-patch-app', $patched->metadata->name);
        self::assertNotNull($patched->spec);
        self::assertSame(3, $patched->spec->replicas);
        self::assertSame('Stopped', $patched->spec->state);
    }

    public function testCreateOrMergePatchCreatesNewResource(): void
    {
        $appRun = new AppRun([
            'metadata' => [
                'name' => 'test-create-apprun',
                'labels' => ['app' => 'KubernetesApiClientFacadeFunctionalTest'],
            ],
            'spec' => [
                'podRef' => [
                    'name' => 'test-pod',
                    'uid' => '550e8400-e29b-41d4-a716-446655440000',
                ],
                'appRef' => [
                    'name' => 'test-app',
                    'appId' => 'app-123',
                    'projectId' => 'project-456',
                ],
                'createdAt' => '2025-01-15T12:00:00Z',
                'state' => 'Running',
            ],
        ]);

        $result = $this->apiClient->createOrMergePatch($appRun);

        self::assertNotNull($result->metadata);
        self::assertSame('test-create-apprun', $result->metadata->name);
        self::assertNotNull($result->spec);
        self::assertSame('Running', $result->spec->state);
    }

    public function testCreateOrMergePatchUpdatesExistingResource(): void
    {
        // Create initial app
        $app = new App([
            'metadata' => [
                'name' => 'test-createorpatch-app',
                'labels' => ['app' => 'KubernetesApiClientFacadeFunctionalTest'],
            ],
            'spec' => [
                'appId' => 'app-123',
                'projectId' => 'project-456',
                'state' => 'Running',
                'replicas' => 1,
                'podSpec' => [
                    'restartPolicy' => 'Always',
                    'containers' => [[
                        'name' => 'main',
                        'image' => 'busybox',
                    ]],
                ],
            ],
        ]);
        $created = $this->apiClient->apps()->create($app);

        // Verify initial state
        self::assertNotNull($created->spec);
        self::assertSame('Running', $created->spec->state);
        self::assertSame(1, $created->spec->replicas);

        // Update via createOrMergePatch (should patch since it exists)
        self::assertNotNull($app->spec);
        $app->spec->replicas = 5;
        $app->spec->state = 'Stopped';

        $result = $this->apiClient->createOrMergePatch($app);

        self::assertNotNull($result->metadata);
        self::assertSame('test-createorpatch-app', $result->metadata->name);
        self::assertNotNull($result->spec);
        self::assertSame(5, $result->spec->replicas);
        self::assertSame('Stopped', $result->spec->state);
    }
}
