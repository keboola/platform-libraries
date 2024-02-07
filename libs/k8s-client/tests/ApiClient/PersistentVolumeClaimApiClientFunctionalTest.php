<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use Keboola\K8sClient\ApiClient\PersistentVolumeClaimsApiClient;
use Kubernetes\API\PersistentVolumeClaim as PersistentVolumeClaimApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\PersistentVolumeClaim;
use PHPUnit\Framework\TestCase;

class PersistentVolumeClaimApiClientFunctionalTest extends TestCase
{
    /**
     * @template-use BaseNamespaceApiClientTestCase<PersistentVolumeClaimApi, PersistentVolumeClaimsApiClient>
     */
    use BaseNamespaceApiClientTestCase;

    public function setUp(): void
    {
        parent::setUp();
        $this->setUpBaseNamespaceApiClientTest(
            PersistentVolumeClaimApi::class,
            PersistentVolumeClaimsApiClient::class,
        );
    }

    protected function createResource(array $metadata): PersistentVolumeClaim
    {
        return new PersistentVolumeClaim([
            'metadata' => $metadata,
            'spec' => [
                'accessModes' => [
                    'ReadWriteOnce',
                ],
                'resources' => [
                    'requests' => [
                        'storage' => '10M',
                    ],
                ],
                'volumeMode' => 'Filesystem',
            ],
        ]);
    }
}
