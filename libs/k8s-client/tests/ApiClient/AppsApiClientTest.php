<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use Keboola\K8sClient\ApiClient\AppsApiClient;
use Keboola\K8sClient\ApiClient\PatchStrategy;
use Keboola\K8sClient\ClientFacadeFactory\ClientConfigurator;
use Keboola\K8sClient\KubernetesApiClient;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\App;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Patch;
use PHPUnit\Framework\TestCase;

class AppsApiClientTest extends TestCase
{
    /**
     * @dataProvider providePatchStrategy
     */
    public function testPatchWithStrategy(?PatchStrategy $strategy, string $expectedOperation): void
    {
        ClientConfigurator::configureBaseClient(
            apiUrl: (string) getenv('K8S_HOST'),
            caCertFile: (string) getenv('K8S_CA_CERT_PATH'),
            token: (string) getenv('K8S_TOKEN'),
        );

        $k8sClientMock = $this->createMock(KubernetesApiClient::class);
        $k8sClientMock->expects(self::once())
            ->method('request')
            ->willReturnCallback(function ($api, $method, $returnClass, $name, $model) use ($expectedOperation) {
                // Verify that patchOperation is set correctly in the model
                self::assertInstanceOf(Patch::class, $model);
                $data = $model->getArrayCopy();
                self::assertArrayHasKey('patchOperation', $data);
                self::assertSame($expectedOperation, $data['patchOperation']);
                self::assertArrayHasKey('spec', $data);
                self::assertSame(3, $data['spec']['replicas']);

                return new App();
            });

        $apiClient = new AppsApiClient($k8sClientMock);

        $patch = new Patch([
            'spec' => [
                'replicas' => 3,
            ],
        ]);

        if ($strategy === null) {
            // Call without strategy parameter - should use default
            $apiClient->patch('test-app', $patch);
        } else {
            // Call with explicit strategy
            $apiClient->patch('test-app', $patch, [], $strategy);
        }
    }

    public static function providePatchStrategy(): iterable
    {
        yield 'default strategy (not specified)' => [
            'strategy' => null,
            'expectedOperation' => 'merge-patch',
        ];

        yield 'JsonPatch' => [
            'strategy' => PatchStrategy::JsonPatch,
            'expectedOperation' => 'patch',
        ];

        yield 'JsonMergePatch' => [
            'strategy' => PatchStrategy::JsonMergePatch,
            'expectedOperation' => 'merge-patch',
        ];

        yield 'StrategicMergePatch' => [
            'strategy' => PatchStrategy::StrategicMergePatch,
            'expectedOperation' => 'strategic-merge-patch',
        ];
    }
}
