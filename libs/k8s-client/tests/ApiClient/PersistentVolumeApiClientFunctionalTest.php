<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use Keboola\K8sClient\ApiClient\PersistentVolumesApiClient;
use Kubernetes\API\PersistentVolume as PersistentVolumeApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\PersistentVolume;
use PHPUnit\Framework\TestCase;

class PersistentVolumeApiClientFunctionalTest extends TestCase
{
    /**
     * @template-use BaseClusterApiClientTestCase<PersistentVolumeApi, PersistentVolumesApiClient>
     */
    use BaseClusterApiClientTestCase;

    public function setUp(): void
    {
        parent::setUp();
        $this->setUpBaseClusterApiClientTest(
            PersistentVolumeApi::class,
            PersistentVolumesApiClient::class,
        );
    }

    protected function createResource(array $metadata): PersistentVolume
    {
        return new PersistentVolume([
            'metadata' => $metadata,
            'spec' => [
                'accessModes' => [
                    'ReadWriteOnce',
                ],
                'capacity' => [
                    'storage' => '10M',
                ],
                'storageClassName' => 'default',
                'volumeMode' => 'Filesystem',
                'persistentVolumeReclaimPolicy' => 'Retain',
                'hostPath' => [
                    'path' => '/home/' . getenv('K8S_NAMESPACE'),
                ],
            ],
        ]);
    }
}
