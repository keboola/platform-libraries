<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests;

use Keboola\K8sClient\ApiClient\AppRunsApiClient;
use Keboola\K8sClient\ApiClient\AppsApiClient;
use Keboola\K8sClient\ApiClient\ConfigMapsApiClient;
use Keboola\K8sClient\ApiClient\EventsApiClient;
use Keboola\K8sClient\ApiClient\IngressesApiClient;
use Keboola\K8sClient\ApiClient\PersistentVolumeClaimsApiClient;
use Keboola\K8sClient\ApiClient\PersistentVolumesApiClient;
use Keboola\K8sClient\ApiClient\PodsApiClient;
use Keboola\K8sClient\ApiClient\SecretsApiClient;
use Keboola\K8sClient\ApiClient\ServicesApiClient;
use Keboola\K8sClient\Exception\ResourceNotFoundException;
use Keboola\K8sClient\Exception\TimeoutException;
use Keboola\K8sClient\KubernetesApiClientFacade;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\App;
use Keboola\K8sClient\PatchStrategy;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Event;
use Kubernetes\Model\Io\K8s\Api\Core\V1\PersistentVolume;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Pod;
use Kubernetes\Model\Io\K8s\Api\Core\V1\PodList;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Secret;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Service;
use Kubernetes\Model\Io\K8s\Api\Networking\V1\Ingress;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Patch;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use RuntimeException;

class KubernetesApiClientFacadeTest extends TestCase
{
    private readonly LoggerInterface $logger;
    private readonly TestHandler $loggerTestHandler;

    protected function setUp(): void
    {
        parent::setUp();

        $this->logger = new Logger('test');
        $this->loggerTestHandler = new TestHandler();
        $this->logger->pushHandler($this->loggerTestHandler);
    }

    public function testApisAccessors(): void
    {
        $configMapsApiClient = $this->createMock(ConfigMapsApiClient::class);
        $eventsApiClient = $this->createMock(EventsApiClient::class);
        $persistentVolumeClaimClient = $this->createMock(PersistentVolumeClaimsApiClient::class);
        $podsApiClient = $this->createMock(PodsApiClient::class);
        $secretsApiClient = $this->createMock(SecretsApiClient::class);
        $servicesApiClient = $this->createMock(ServicesApiClient::class);
        $ingressesApiClient = $this->createMock(IngressesApiClient::class);
        $persistentVolumeClient = $this->createMock(PersistentVolumesApiClient::class);

        $appsApiClient = $this->createMock(AppsApiClient::class);
        $appRunsApiClient = $this->createMock(AppRunsApiClient::class);

        $facade = new KubernetesApiClientFacade(
            $this->logger,
            $configMapsApiClient,
            $eventsApiClient,
            $ingressesApiClient,
            $persistentVolumeClaimClient,
            $persistentVolumeClient,
            $podsApiClient,
            $secretsApiClient,
            $servicesApiClient,
            $appsApiClient,
            $appRunsApiClient,
        );

        self::assertSame($configMapsApiClient, $facade->configMaps());
        self::assertSame($eventsApiClient, $facade->events());
        self::assertSame($persistentVolumeClaimClient, $facade->persistentVolumeClaims());
        self::assertSame($podsApiClient, $facade->pods());
        self::assertSame($secretsApiClient, $facade->secrets());
        self::assertSame($ingressesApiClient, $facade->ingresses());
        self::assertSame($persistentVolumeClient, $facade->persistentVolumes());
        self::assertSame($appsApiClient, $facade->apps());
        self::assertSame($appRunsApiClient, $facade->appRuns());
    }

    public function testGetPod(): void
    {
        $returnedPod = new Pod([
            'metadata' => [
                'name' => 'pod-name',
                'labels' => [
                    'app' => 'pod-name',
                ],
            ],
        ]);

        $podsApiClient = $this->createMock(PodsApiClient::class);
        $podsApiClient->expects(self::once())
            ->method('get')
            ->with('pod-name', ['labelSelector' => 'app=pod-name'])
            ->willReturn($returnedPod)
        ;

        $secretsApiClient = $this->createMock(SecretsApiClient::class);
        $secretsApiClient->expects(self::never())->method(self::anything());

        $eventsApiClient = $this->createMock(EventsApiClient::class);
        $eventsApiClient->expects(self::never())->method(self::anything());

        $servicesApiClient = $this->createMock(ServicesApiClient::class);
        $servicesApiClient->expects(self::never())->method(self::anything());

        $ingressesApiClient = $this->createMock(IngressesApiClient::class);
        $ingressesApiClient->expects(self::never())->method(self::anything());

        $persistentVolumesApiClient = $this->createMock(PersistentVolumesApiClient::class);
        $persistentVolumesApiClient->expects(self::never())->method(self::anything());

        $facade = new KubernetesApiClientFacade(
            $this->logger,
            $this->createMock(ConfigMapsApiClient::class),
            $eventsApiClient,
            $ingressesApiClient,
            $this->createMock(PersistentVolumeClaimsApiClient::class),
            $persistentVolumesApiClient,
            $podsApiClient,
            $secretsApiClient,
            $servicesApiClient,
            $this->createMock(AppsApiClient::class),
            $this->createMock(AppRunsApiClient::class),
        );

        $result = $facade->get(Pod::class, 'pod-name', ['labelSelector' => 'app=pod-name']);
        self::assertSame($returnedPod, $result);
    }

    public function testGetSecret(): void
    {
        $returnedSecret = new Secret([
            'metadata' => [
                'name' => 'secret-name',
                'labels' => [
                    'app' => 'secret-name',
                ],
            ],
        ]);

        $podsApiClient = $this->createMock(PodsApiClient::class);
        $podsApiClient->expects(self::never())->method(self::anything());

        $secretsApiClient = $this->createMock(SecretsApiClient::class);
        $secretsApiClient->expects(self::once())
            ->method('get')
            ->with('secret-name', ['labelSelector' => 'app=secret-name'])
            ->willReturn($returnedSecret)
        ;

        $eventsApiClient = $this->createMock(EventsApiClient::class);
        $eventsApiClient->expects(self::never())->method(self::anything());

        $servicesApiClient = $this->createMock(ServicesApiClient::class);
        $servicesApiClient->expects(self::never())->method(self::anything());

        $ingressesApiClient = $this->createMock(IngressesApiClient::class);
        $ingressesApiClient->expects(self::never())->method(self::anything());

        $persistentVolumesApiClient = $this->createMock(PersistentVolumesApiClient::class);
        $persistentVolumesApiClient->expects(self::never())->method(self::anything());

        $facade = new KubernetesApiClientFacade(
            $this->logger,
            $this->createMock(ConfigMapsApiClient::class),
            $eventsApiClient,
            $ingressesApiClient,
            $this->createMock(PersistentVolumeClaimsApiClient::class),
            $persistentVolumesApiClient,
            $podsApiClient,
            $secretsApiClient,
            $servicesApiClient,
            $this->createMock(AppsApiClient::class),
            $this->createMock(AppRunsApiClient::class),
        );

        $result = $facade->get(Secret::class, 'secret-name', ['labelSelector' => 'app=secret-name']);
        self::assertSame($returnedSecret, $result);
    }

    public function testGetEvent(): void
    {
        $returnedEvent = new Event([
            'metadata' => [
                'name' => 'event-name',
                'labels' => [
                    'app' => 'event-name',
                ],
            ],
        ]);

        $podsApiClient = $this->createMock(PodsApiClient::class);
        $podsApiClient->expects(self::never())->method(self::anything());

        $secretsApiClient = $this->createMock(SecretsApiClient::class);
        $secretsApiClient->expects(self::never())->method(self::anything());

        $eventsApiClient = $this->createMock(EventsApiClient::class);
        $eventsApiClient->expects(self::once())
            ->method('get')
            ->with('event-name', ['labelSelector' => 'app=event-name'])
            ->willReturn($returnedEvent)
        ;

        $servicesApiClient = $this->createMock(ServicesApiClient::class);
        $servicesApiClient->expects(self::never())->method(self::anything());

        $ingressesApiClient = $this->createMock(IngressesApiClient::class);
        $ingressesApiClient->expects(self::never())->method(self::anything());

        $persistentVolumesApiClient = $this->createMock(PersistentVolumesApiClient::class);
        $persistentVolumesApiClient->expects(self::never())->method(self::anything());

        $facade = new KubernetesApiClientFacade(
            $this->logger,
            $this->createMock(ConfigMapsApiClient::class),
            $eventsApiClient,
            $ingressesApiClient,
            $this->createMock(PersistentVolumeClaimsApiClient::class),
            $persistentVolumesApiClient,
            $podsApiClient,
            $secretsApiClient,
            $servicesApiClient,
            $this->createMock(AppsApiClient::class),
            $this->createMock(AppRunsApiClient::class),
        );

        $result = $facade->get(Event::class, 'event-name', ['labelSelector' => 'app=event-name']);
        self::assertSame($returnedEvent, $result);
    }

    public function testCreateModels(): void
    {
        // request & result represent the same resource but are different class instances
        $podRequest1 = new Pod(['metadata' => ['name' => 'pod1']]);
        $podRequest2 = new Pod(['metadata' => ['name' => 'pod2']]);
        $secretRequest3 = new Secret(['metadata' => ['name' => 'secret3']]);
        $eventRequest4 = new Event(['metadata' => ['name' => 'event4']]);
        $serviceRequest5 = new Service(['metadata' => ['name' => 'service5']]);
        $ingressRequest6 = new Ingress(['metadata' => ['name' => 'ingress6']]);
        $persistentVolumeRequest7 = new PersistentVolume(['metadata' => ['name' => 'persistentVolume7']]);

        $podResult1 = new Pod(['metadata' => ['name' => 'pod1']]);
        $podResult2 = new Pod(['metadata' => ['name' => 'pod2']]);
        $secretResult3 = new Secret(['metadata' => ['name' => 'secret3']]);
        $eventResult4 = new Event(['metadata' => ['name' => 'event4']]);
        $serviceResult5 = new Service(['metadata' => ['name' => 'service5']]);
        $ingressResult6 = new Ingress(['metadata' => ['name' => 'ingress6']]);
        $persistentVolumeResult7 = new PersistentVolume(['metadata' => ['name' => 'persistentVolume7']]);

        $podsApiClient = $this->createMock(PodsApiClient::class);
        $podsApiClient->expects(self::exactly(2))
            ->method('create')
            ->withConsecutive(
                [$podRequest1, []],
                [$podRequest2, []],
            )
            ->willReturnOnConsecutiveCalls(
                $podResult1,
                $podResult2,
                $secretResult3,
            )
        ;

        $secretsApiClient = $this->createMock(SecretsApiClient::class);
        $secretsApiClient->expects(self::once())
            ->method('create')
            ->with($secretRequest3, [])
            ->willReturn($secretResult3)
        ;

        $eventsApiClient = $this->createMock(EventsApiClient::class);
        $eventsApiClient->expects(self::once())
            ->method('create')
            ->with($eventRequest4, [])
            ->willReturn($eventResult4)
        ;

        $servicesApiClient = $this->createMock(ServicesApiClient::class);
        $servicesApiClient->expects(self::once())
            ->method('create')
            ->with($serviceRequest5, [])
            ->willReturn($serviceResult5)
        ;

        $ingressesApiClient = $this->createMock(IngressesApiClient::class);
        $ingressesApiClient->expects(self::once())
            ->method('create')
            ->with($ingressRequest6, [])
            ->willReturn($ingressResult6)
        ;

        $persistentVolumesApiClient = $this->createMock(PersistentVolumesApiClient::class);
        $persistentVolumesApiClient->expects(self::once())
            ->method('create')
            ->with($persistentVolumeRequest7, [])
            ->willReturn($persistentVolumeResult7)
        ;

        $facade = new KubernetesApiClientFacade(
            $this->logger,
            $this->createMock(ConfigMapsApiClient::class),
            $eventsApiClient,
            $ingressesApiClient,
            $this->createMock(PersistentVolumeClaimsApiClient::class),
            $persistentVolumesApiClient,
            $podsApiClient,
            $secretsApiClient,
            $servicesApiClient,
            $this->createMock(AppsApiClient::class),
            $this->createMock(AppRunsApiClient::class),
        );

        $result = $facade->createModels([
            $podRequest1,
            $podRequest2,
            $secretRequest3,
            $eventRequest4,
            $serviceRequest5,
            $ingressRequest6,
            $persistentVolumeRequest7,
        ]);

        self::assertSame([
            $podResult1,
            $podResult2,
            $secretResult3,
            $eventResult4,
            $serviceResult5,
            $ingressResult6,
            $persistentVolumeResult7,
        ], $result);
    }

    public function testCreateModelsErrorHandling(): void
    {
        $pod1 = new Pod(['metadata' => ['name' => 'pod1']]);
        $pod2 = new Pod(['metadata' => ['name' => 'pod2']]);
        $pod3 = new Pod(['metadata' => ['name' => 'pod3']]);

        $podsApiClient = $this->createMock(PodsApiClient::class);
        $podsApiClient->expects(self::exactly(2))
            ->method('create')
            ->withConsecutive(
                [$pod1, []],
                [$pod2, []],
            )
            ->will(self::onConsecutiveCalls(
                self::returnArgument(0),
                self::throwException(new RuntimeException('Can\'t create Pod')),
            ))
        ;

        $secretsApiClient = $this->createMock(SecretsApiClient::class);
        $secretsApiClient->expects(self::never())->method(self::anything());

        $eventsApiClient = $this->createMock(EventsApiClient::class);
        $eventsApiClient->expects(self::never())->method(self::anything());

        $servicesApiClient = $this->createMock(ServicesApiClient::class);
        $servicesApiClient->expects(self::never())->method(self::anything());

        $ingressesApiClient = $this->createMock(IngressesApiClient::class);
        $ingressesApiClient->expects(self::never())->method(self::anything());

        $persistentVolumesApiClient = $this->createMock(PersistentVolumesApiClient::class);
        $persistentVolumesApiClient->expects(self::never())->method(self::anything());

        $facade = new KubernetesApiClientFacade(
            $this->logger,
            $this->createMock(ConfigMapsApiClient::class),
            $eventsApiClient,
            $ingressesApiClient,
            $this->createMock(PersistentVolumeClaimsApiClient::class),
            $persistentVolumesApiClient,
            $podsApiClient,
            $secretsApiClient,
            $servicesApiClient,
            $this->createMock(AppsApiClient::class),
            $this->createMock(AppRunsApiClient::class),
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Can\'t create Pod');

        $facade->createModels([$pod1, $pod2, $pod3]);
    }

    public function testDeleteModels(): void
    {
        // request & result represent the same resource but are different class instances
        $podRequest1 = new Pod(['metadata' => ['name' => 'pod1']]);
        $podRequest2 = new Pod(['metadata' => ['name' => 'pod2']]);
        $secretRequest3 = new Secret(['metadata' => ['name' => 'secret3']]);
        $eventRequest4 = new Event(['metadata' => ['name' => 'event4']]);
        $serviceRequest5 = new Service(['metadata' => ['name' => 'service5']]);
        $ingressRequest6 = new Ingress(['metadata' => ['name' => 'ingress6']]);
        $persistentVolumeRequest7 = new PersistentVolume(['metadata' => ['name' => 'persistentVolume7']]);

        $podResult1 = new Status(['metadata' => ['name' => 'pod1']]);
        $podResult2 = new Status(['metadata' => ['name' => 'pod2']]);
        $secretResult3 = new Status(['metadata' => ['name' => 'secret3']]);
        $eventResult4 = new Status(['metadata' => ['name' => 'event4']]);
        $serviceResult5 = new Status(['metadata' => ['name' => 'service5']]);
        $ingressResult6 = new Status(['metadata' => ['name' => 'ingress6']]);
        $persistentVolumeResult7 = new Status(['metadata' => ['name' => 'persistentVolume7']]);

        $deleteOptions = new DeleteOptions();

        $podsApiClient = $this->createMock(PodsApiClient::class);
        $podsApiClient->expects(self::exactly(2))
            ->method('delete')
            ->withConsecutive(
                ['pod1', $deleteOptions, []],
                ['pod2', $deleteOptions, []],
            )
            ->willReturnOnConsecutiveCalls(
                $podResult1,
                $podResult2,
            )
        ;

        $secretsApiClient = $this->createMock(SecretsApiClient::class);
        $secretsApiClient->expects(self::once())
            ->method('delete')
            ->with('secret3', $deleteOptions, [])
            ->willReturn($secretResult3)
        ;

        $eventsApiClient = $this->createMock(EventsApiClient::class);
        $eventsApiClient->expects(self::once())
            ->method('delete')
            ->with('event4', $deleteOptions, [])
            ->willReturn($eventResult4)
        ;

        $servicesApiClient = $this->createMock(ServicesApiClient::class);
        $servicesApiClient->expects(self::once())
            ->method('delete')
            ->with('service5', $deleteOptions, [])
            ->willReturn($serviceResult5)
        ;

        $ingressesApiClient = $this->createMock(IngressesApiClient::class);
        $ingressesApiClient->expects(self::once())
            ->method('delete')
            ->with('ingress6', $deleteOptions, [])
            ->willReturn($ingressResult6)
        ;

        $persistentVolumesApiClient = $this->createMock(PersistentVolumesApiClient::class);
        $persistentVolumesApiClient->expects(self::once())
            ->method('delete')
            ->with('persistentVolume7', $deleteOptions, [])
            ->willReturn($persistentVolumeResult7)
        ;

        $facade = new KubernetesApiClientFacade(
            $this->logger,
            $this->createMock(ConfigMapsApiClient::class),
            $eventsApiClient,
            $ingressesApiClient,
            $this->createMock(PersistentVolumeClaimsApiClient::class),
            $persistentVolumesApiClient,
            $podsApiClient,
            $secretsApiClient,
            $servicesApiClient,
            $this->createMock(AppsApiClient::class),
            $this->createMock(AppRunsApiClient::class),
        );

        $result = $facade->deleteModels([
            $podRequest1,
            $podRequest2,
            $secretRequest3,
            $eventRequest4,
            $serviceRequest5,
            $ingressRequest6,
            $persistentVolumeRequest7,
        ], $deleteOptions);

        self::assertSame([
            $podResult1,
            $podResult2,
            $secretResult3,
            $eventResult4,
            $serviceResult5,
            $ingressResult6,
            $persistentVolumeResult7,
        ], $result);
    }

    public function testDeleteModelsErrorHandling(): void
    {
        $pod1 = new Pod(['metadata' => ['name' => 'pod1']]);
        $pod2 = new Pod(['metadata' => ['name' => 'pod2']]);
        $pod3 = new Pod(['metadata' => ['name' => 'pod3']]);

        $deleteOptions = new DeleteOptions();

        $podsApiClient = $this->createMock(PodsApiClient::class);
        $podsApiClient->expects(self::exactly(2))
            ->method('delete')
            ->withConsecutive(
                ['pod1', $deleteOptions, []],
                ['pod2', $deleteOptions, []],
            )
            ->will(self::onConsecutiveCalls(
                new Status(['metadata' => ['name' => 'pod1']]),
                self::throwException(new RuntimeException('Can\'t delete Pod')),
            ))
        ;

        $secretsApiClient = $this->createMock(SecretsApiClient::class);
        $secretsApiClient->expects(self::never())->method(self::anything());

        $eventsApiClient = $this->createMock(EventsApiClient::class);
        $eventsApiClient->expects(self::never())->method(self::anything());

        $servicesApiClient = $this->createMock(ServicesApiClient::class);
        $servicesApiClient->expects(self::never())->method(self::anything());

        $ingressesApiClient = $this->createMock(IngressesApiClient::class);
        $ingressesApiClient->expects(self::never())->method(self::anything());

        $persistentVolumesApiClient = $this->createMock(PersistentVolumesApiClient::class);
        $persistentVolumesApiClient->expects(self::never())->method(self::anything());

        $facade = new KubernetesApiClientFacade(
            $this->logger,
            $this->createMock(ConfigMapsApiClient::class),
            $eventsApiClient,
            $ingressesApiClient,
            $this->createMock(PersistentVolumeClaimsApiClient::class),
            $persistentVolumesApiClient,
            $podsApiClient,
            $secretsApiClient,
            $servicesApiClient,
            $this->createMock(AppsApiClient::class),
            $this->createMock(AppRunsApiClient::class),
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Can\'t delete Pod');

        $facade->deleteModels([$pod1, $pod2, $pod3], $deleteOptions);
    }

    public function testWaitWhileExists(): void
    {
        $podsApiClient = $this->createMock(PodsApiClient::class);
        $podsApiClient->expects(self::exactly(4))
            ->method('get')
            ->withConsecutive(
                // first round check both pods, pod1 still exists, pod2 does not exist
                ['pod1'],
                ['pod2'],
                // second round checks remaining pod1
                ['pod1'],
                // third round checks remaining pod1
                ['pod1'],
            )
            ->will(self::onConsecutiveCalls(
                new Pod(['metadata' => ['name' => 'pod1']]),
                self::throwException(new ResourceNotFoundException('Pod doesn\'t exist', null)),
                new Pod(['metadata' => ['name' => 'pod1']]),
                self::throwException(new ResourceNotFoundException('Pod doesn\'t exist', null)),
            ))
        ;

        $secretsApiClient = $this->createMock(SecretsApiClient::class);
        $secretsApiClient->expects(self::never())->method(self::anything());

        $eventsApiClient = $this->createMock(EventsApiClient::class);
        $eventsApiClient->expects(self::never())->method(self::anything());

        $servicesApiClient = $this->createMock(ServicesApiClient::class);
        $servicesApiClient->expects(self::never())->method(self::anything());

        $ingressesApiClient = $this->createMock(IngressesApiClient::class);
        $ingressesApiClient->expects(self::never())->method(self::anything());

        $persistentVolumesApiClient = $this->createMock(PersistentVolumesApiClient::class);
        $persistentVolumesApiClient->expects(self::never())->method(self::anything());

        $facade = new KubernetesApiClientFacade(
            $this->logger,
            $this->createMock(ConfigMapsApiClient::class),
            $eventsApiClient,
            $ingressesApiClient,
            $this->createMock(PersistentVolumeClaimsApiClient::class),
            $persistentVolumesApiClient,
            $podsApiClient,
            $secretsApiClient,
            $servicesApiClient,
            $this->createMock(AppsApiClient::class),
            $this->createMock(AppRunsApiClient::class),
        );

        $facade->waitWhileExists([
            new Pod(['metadata' => ['name' => 'pod1']]),
            new Pod(['metadata' => ['name' => 'pod2']]),
        ]);
    }

    public function testWaitWhileExistsTimeout(): void
    {
        $podsApiClient = $this->createMock(PodsApiClient::class);
        $podsApiClient
            ->method('get')
            ->willReturnCallback(fn($podName) => new Pod(['metadata' => ['name' => $podName]]))
        ;

        $secretsApiClient = $this->createMock(SecretsApiClient::class);
        $secretsApiClient->expects(self::never())->method(self::anything());

        $eventsApiClient = $this->createMock(EventsApiClient::class);
        $eventsApiClient->expects(self::never())->method(self::anything());

        $servicesApiClient = $this->createMock(ServicesApiClient::class);
        $servicesApiClient->expects(self::never())->method(self::anything());

        $ingressesApiClient = $this->createMock(IngressesApiClient::class);
        $ingressesApiClient->expects(self::never())->method(self::anything());

        $persistentVolumesApiClient = $this->createMock(PersistentVolumesApiClient::class);
        $persistentVolumesApiClient->expects(self::never())->method(self::anything());

        $facade = new KubernetesApiClientFacade(
            $this->logger,
            $this->createMock(ConfigMapsApiClient::class),
            $eventsApiClient,
            $ingressesApiClient,
            $this->createMock(PersistentVolumeClaimsApiClient::class),
            $persistentVolumesApiClient,
            $podsApiClient,
            $secretsApiClient,
            $servicesApiClient,
            $this->createMock(AppsApiClient::class),
            $this->createMock(AppRunsApiClient::class),
        );

        $startTime = microtime(true);
        try {
            $facade->waitWhileExists([
                new Pod(['metadata' => ['name' => 'pod1']]),
                new Pod(['metadata' => ['name' => 'pod2']]),
            ], 3);
            self::fail('Expected TimeoutException was not thrown');
        } catch (TimeoutException) {
        }
        $endTime = microtime(true);

        self::assertEqualsWithDelta(3, $endTime - $startTime, 1);
    }

    public function testListMatching(): void
    {
        $pod1 = new Pod(['metadata' => ['name' => 'pod-1']]);
        $pod2 = new Pod(['metadata' => ['name' => 'pod-2']]);
        $pod3 = new Pod(['metadata' => ['name' => 'pod-3']]);

        $podsApiClient = $this->createMock(PodsApiClient::class);
        $podsApiClient->expects(self::exactly(2))
            ->method('list')
            ->withConsecutive(
                [['labelSelector' => 'app=my', 'limit' => 100]],
                [['labelSelector' => 'app=my', 'limit' => 100, 'continue' => 'foo']],
            )
            ->willReturnOnConsecutiveCalls(
                new PodList([
                    'metadata' => [
                        'continue' => 'foo',
                    ],
                    'items' => [$pod1, $pod2],
                ]),
                new PodList([
                    'items' => [$pod3],
                ]),
            )
        ;

        $secretsApiClient = $this->createMock(SecretsApiClient::class);
        $secretsApiClient->expects(self::never())->method(self::anything());

        $eventsApiClient = $this->createMock(EventsApiClient::class);
        $eventsApiClient->expects(self::never())->method(self::anything());

        $servicesApiClient = $this->createMock(ServicesApiClient::class);
        $servicesApiClient->expects(self::never())->method(self::anything());

        $ingressesApiClient = $this->createMock(IngressesApiClient::class);
        $ingressesApiClient->expects(self::never())->method(self::anything());

        $persistentVolumesApiClient = $this->createMock(PersistentVolumesApiClient::class);
        $persistentVolumesApiClient->expects(self::never())->method(self::anything());

        $facade = new KubernetesApiClientFacade(
            $this->logger,
            $this->createMock(ConfigMapsApiClient::class),
            $eventsApiClient,
            $ingressesApiClient,
            $this->createMock(PersistentVolumeClaimsApiClient::class),
            $persistentVolumesApiClient,
            $podsApiClient,
            $secretsApiClient,
            $servicesApiClient,
            $this->createMock(AppsApiClient::class),
            $this->createMock(AppRunsApiClient::class),
        );

        $result = $facade->listMatching(Pod::class, ['labelSelector' => 'app=my']);
        self::assertEquals([$pod1, $pod2, $pod3], [...$result]);
    }

    public function testListMatchingWithCustomPageSize(): void
    {
        $pod1 = new Pod(['metadata' => ['name' => 'pod-1']]);
        $pod2 = new Pod(['metadata' => ['name' => 'pod-2']]);
        $pod3 = new Pod(['metadata' => ['name' => 'pod-3']]);

        $podsApiClient = $this->createMock(PodsApiClient::class);
        $podsApiClient->expects(self::exactly(2))
            ->method('list')
            ->withConsecutive(
                [['labelSelector' => 'app=my', 'limit' => 5]],
                [['labelSelector' => 'app=my', 'limit' => 5, 'continue' => 'foo']],
            )
            ->willReturnOnConsecutiveCalls(
                new PodList([
                    'metadata' => [
                        'continue' => 'foo',
                    ],
                    'items' => [$pod1, $pod2],
                ]),
                new PodList([
                    'items' => [$pod3],
                ]),
            )
        ;

        $secretsApiClient = $this->createMock(SecretsApiClient::class);
        $secretsApiClient->expects(self::never())->method(self::anything());

        $eventsApiClient = $this->createMock(EventsApiClient::class);
        $eventsApiClient->expects(self::never())->method(self::anything());

        $servicesApiClient = $this->createMock(ServicesApiClient::class);
        $servicesApiClient->expects(self::never())->method(self::anything());

        $ingressesApiClient = $this->createMock(IngressesApiClient::class);
        $ingressesApiClient->expects(self::never())->method(self::anything());

        $persistentVolumesApiClient = $this->createMock(PersistentVolumesApiClient::class);
        $persistentVolumesApiClient->expects(self::never())->method(self::anything());

        $facade = new KubernetesApiClientFacade(
            $this->logger,
            $this->createMock(ConfigMapsApiClient::class),
            $eventsApiClient,
            $ingressesApiClient,
            $this->createMock(PersistentVolumeClaimsApiClient::class),
            $persistentVolumesApiClient,
            $podsApiClient,
            $secretsApiClient,
            $servicesApiClient,
            $this->createMock(AppsApiClient::class),
            $this->createMock(AppRunsApiClient::class),
        );

        $result = $facade->listMatching(Pod::class, ['labelSelector' => 'app=my', 'limit' => 5]);
        self::assertEquals([$pod1, $pod2, $pod3], [...$result]);
    }

    public function testDeleteAllMatching(): void
    {
        $deleteOptions = new DeleteOptions();
        $deleteQuery = ['labelSelector' => 'app=my-app'];

        $configMapsApiClient = $this->createMock(ConfigMapsApiClient::class);
        $configMapsApiClient->expects(self::once())
            ->method('deleteCollection')
            ->with($deleteOptions, $deleteQuery)
        ;

        $pvClaimApiClient = $this->createMock(PersistentVolumeClaimsApiClient::class);
        $pvClaimApiClient->expects(self::once())
            ->method('deleteCollection')
            ->with($deleteOptions, $deleteQuery)
        ;

        $podsApiClient = $this->createMock(PodsApiClient::class);
        $podsApiClient->expects(self::once())
            ->method('deleteCollection')
            ->with($deleteOptions, $deleteQuery)
        ;

        $secretsApiClient = $this->createMock(SecretsApiClient::class);
        $secretsApiClient->expects(self::once())
            ->method('deleteCollection')
            ->with($deleteOptions, $deleteQuery)
        ;

        $eventsApiClient = $this->createMock(EventsApiClient::class);
        $eventsApiClient->expects(self::never())->method(self::anything());

        $servicesApiClient = $this->createMock(ServicesApiClient::class);
        $servicesApiClient->expects(self::once())
            ->method('deleteCollection')
            ->with($deleteOptions, $deleteQuery)
        ;

        $ingressesApiClient = $this->createMock(IngressesApiClient::class);
        $ingressesApiClient->expects(self::once())
            ->method('deleteCollection')
            ->with($deleteOptions, $deleteQuery)
        ;

        $persistentVolumesApiClient = $this->createMock(PersistentVolumesApiClient::class);
        $persistentVolumesApiClient->expects(self::once())
            ->method('deleteCollection')
            ->with($deleteOptions, $deleteQuery)
        ;

        $appsApiClient = $this->createMock(AppsApiClient::class);
        $appsApiClient->expects(self::once())
            ->method('deleteCollection')
            ->with($deleteOptions, $deleteQuery)
        ;

        $appRunsApiClient = $this->createMock(AppRunsApiClient::class);
        $appRunsApiClient->expects(self::once())
            ->method('deleteCollection')
            ->with($deleteOptions, $deleteQuery)
        ;

        $facade = new KubernetesApiClientFacade(
            $this->logger,
            $configMapsApiClient,
            $eventsApiClient,
            $ingressesApiClient,
            $pvClaimApiClient,
            $persistentVolumesApiClient,
            $podsApiClient,
            $secretsApiClient,
            $servicesApiClient,
            $appsApiClient,
            $appRunsApiClient,
        );

        $facade->deleteAllMatching($deleteOptions, $deleteQuery);
    }

    public function testDeleteAllMatchingWithResourceTypesFilter(): void
    {
        $deleteOptions = new DeleteOptions();
        $deleteQuery = ['labelSelector' => 'app=my-app'];

        $configMapsApiClient = $this->createMock(ConfigMapsApiClient::class);
        $configMapsApiClient->expects(self::never())->method(self::anything());

        $pvClaimApiClient = $this->createMock(PersistentVolumeClaimsApiClient::class);
        $pvClaimApiClient->expects(self::never())->method(self::anything());

        $podsApiClient = $this->createMock(PodsApiClient::class);
        $podsApiClient->expects(self::never())->method(self::anything());

        $secretsApiClient = $this->createMock(SecretsApiClient::class);
        $secretsApiClient->expects(self::once())
            ->method('deleteCollection')
            ->with($deleteOptions, $deleteQuery)
        ;

        $eventsApiClient = $this->createMock(EventsApiClient::class);
        $eventsApiClient->expects(self::never())->method(self::anything());

        $servicesApiClient = $this->createMock(ServicesApiClient::class);
        $servicesApiClient->expects(self::never())->method(self::anything());

        $ingressesApiClient = $this->createMock(IngressesApiClient::class);
        $ingressesApiClient->expects(self::never())->method(self::anything());

        $persistentVolumesApiClient = $this->createMock(PersistentVolumesApiClient::class);
        $persistentVolumesApiClient->expects(self::never())->method(self::anything());

        $facade = new KubernetesApiClientFacade(
            $this->logger,
            $configMapsApiClient,
            $eventsApiClient,
            $ingressesApiClient,
            $pvClaimApiClient,
            $persistentVolumesApiClient,
            $podsApiClient,
            $secretsApiClient,
            $servicesApiClient,
            $this->createMock(AppsApiClient::class),
            $this->createMock(AppRunsApiClient::class),
        );

        $facade->deleteAllMatching($deleteOptions, ['resourceTypes' => [Secret::class], ...$deleteQuery]);
    }

    public function testDeleteAllMatchingErrorHandling(): void
    {
        $deleteOptions = new DeleteOptions();
        $deleteQuery = ['labelSelector' => 'app=my-app'];

        $configMapsApiClient = $this->createMock(ConfigMapsApiClient::class);
        $configMapsApiClient->expects(self::once())
            ->method('deleteCollection')
            ->willThrowException(new RuntimeException('Config map delete failed'))
        ;

        $pvClaimApiClient = $this->createMock(PersistentVolumeClaimsApiClient::class);
        $pvClaimApiClient->expects(self::once())
            ->method('deleteCollection')
            ->with($deleteOptions, $deleteQuery)
        ;

        $podsApiClient = $this->createMock(PodsApiClient::class);
        $podsApiClient->expects(self::once())
            ->method('deleteCollection')
            ->with($deleteOptions, $deleteQuery)
            ->willThrowException(new RuntimeException('Pod delete failed'))
        ;

        $secretsApiClient = $this->createMock(SecretsApiClient::class);
        $secretsApiClient->expects(self::once())
            ->method('deleteCollection')
            ->with($deleteOptions, $deleteQuery)
        ;

        $eventsApiClient = $this->createMock(EventsApiClient::class);
        $eventsApiClient->expects(self::never())->method(self::anything());

        $servicesApiClient = $this->createMock(ServicesApiClient::class);
        $servicesApiClient->expects(self::once())
            ->method('deleteCollection')
            ->with($deleteOptions, $deleteQuery)
        ;

        $ingressesApiClient = $this->createMock(IngressesApiClient::class);
        $ingressesApiClient->expects(self::once())
            ->method('deleteCollection')
            ->with($deleteOptions, $deleteQuery)
        ;

        $persistentVolumesApiClient = $this->createMock(PersistentVolumesApiClient::class);
        $persistentVolumesApiClient->expects(self::once())
            ->method('deleteCollection')
            ->with($deleteOptions, $deleteQuery)
        ;

        $appsApiClient = $this->createMock(AppsApiClient::class);
        $appsApiClient->expects(self::once())
            ->method('deleteCollection')
            ->with($deleteOptions, $deleteQuery)
        ;

        $appRunsApiClient = $this->createMock(AppRunsApiClient::class);
        $appRunsApiClient->expects(self::once())
            ->method('deleteCollection')
            ->with($deleteOptions, $deleteQuery)
        ;

        $facade = new KubernetesApiClientFacade(
            $this->logger,
            $configMapsApiClient,
            $eventsApiClient,
            $ingressesApiClient,
            $pvClaimApiClient,
            $persistentVolumesApiClient,
            $podsApiClient,
            $secretsApiClient,
            $servicesApiClient,
            $appsApiClient,
            $appRunsApiClient,
        );

        try {
            $facade->deleteAllMatching($deleteOptions, $deleteQuery);
            $this->fail('RuntimeException should be thrown on deleteAllMatching call.');
        } catch (RuntimeException $e) {
            self::assertEquals('Pod delete failed', $e->getMessage());
        }

        $records = $this->loggerTestHandler->getRecords();
        self::assertCount(2, $records);

        $record = $records[0];
        self::assertEquals(400, $record['level']);
        self::assertEquals('DeleteCollection request has failed', $record['message']);
        self::assertIsArray($record['context']);
        self::assertArrayHasKey('exception', $record['context']);
        self::assertInstanceOf(RuntimeException::class, $record['context']['exception']);
        self::assertSame('Config map delete failed', $record['context']['exception']->getMessage());

        $record = $records[1];
        self::assertEquals(400, $record['level']);
        self::assertEquals('DeleteCollection request has failed', $record['message']);
        self::assertIsArray($record['context']);
        self::assertArrayHasKey('exception', $record['context']);
        self::assertInstanceOf(RuntimeException::class, $record['context']['exception']);
        self::assertSame('Pod delete failed', $record['context']['exception']->getMessage());
    }

    public function testCheckResourceExists(): void
    {
        $podsApiClient = $this->createMock(PodsApiClient::class);
        $podsApiClient->expects(self::once())
            ->method('get')
            ->with('pod-name')
            ->willReturn($this->createMock(Pod::class))
        ;

        $secretsApiClient = $this->createMock(SecretsApiClient::class);
        $secretsApiClient->expects(self::once())
            ->method('get')
            ->with('secret-name')
            ->willThrowException(new ResourceNotFoundException('Secret not found', null))
        ;

        $facade = new KubernetesApiClientFacade(
            $this->logger,
            $this->createMock(ConfigMapsApiClient::class),
            $this->createMock(EventsApiClient::class),
            $this->createMock(IngressesApiClient::class),
            $this->createMock(PersistentVolumeClaimsApiClient::class),
            $this->createMock(PersistentVolumesApiClient::class),
            $podsApiClient,
            $secretsApiClient,
            $this->createMock(ServicesApiClient::class),
            $this->createMock(AppsApiClient::class),
            $this->createMock(AppRunsApiClient::class),
        );

        self::assertFalse($facade->checkResourceExists(Secret::class, 'secret-name'));
        self::assertTrue($facade->checkResourceExists(Pod::class, 'pod-name'));
    }

    /**
     * @dataProvider providePatchStrategy
     */
    public function testPatchWithStrategy(?PatchStrategy $strategy, string $expectedOperation): void
    {
        $app = new App([
            'metadata' => ['name' => 'test-app'],
            'spec' => ['replicas' => 3],
        ]);

        $appsApiClient = $this->createMock(AppsApiClient::class);
        $appsApiClient->expects(self::once())
            ->method('patch')
            ->willReturnCallback(function ($name, $patch) use ($expectedOperation, $app) {
                self::assertInstanceOf(Patch::class, $patch);
                $data = $patch->getArrayCopy();
                self::assertArrayHasKey('patchOperation', $data);
                self::assertSame($expectedOperation, $data['patchOperation']);
                return $app;
            });

        $facade = new KubernetesApiClientFacade(
            $this->logger,
            $this->createMock(ConfigMapsApiClient::class),
            $this->createMock(EventsApiClient::class),
            $this->createMock(IngressesApiClient::class),
            $this->createMock(PersistentVolumeClaimsApiClient::class),
            $this->createMock(PersistentVolumesApiClient::class),
            $this->createMock(PodsApiClient::class),
            $this->createMock(SecretsApiClient::class),
            $this->createMock(ServicesApiClient::class),
            $appsApiClient,
            $this->createMock(AppRunsApiClient::class),
        );

        if ($strategy === null) {
            $result = $facade->patch($app);
        } else {
            $result = $facade->patch($app, $strategy);
        }

        self::assertSame($app, $result);
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
