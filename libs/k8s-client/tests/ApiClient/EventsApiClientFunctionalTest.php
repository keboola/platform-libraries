<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use Keboola\K8sClient\ApiClient\EventsApiClient;
use Keboola\K8sClient\ClientFacadeFactory\GenericClientFacadeFactory;
use Keboola\K8sClient\KubernetesApiClientFacade;
use Keboola\K8sClient\RetryProxyFactory;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Event;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Pod;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

class EventsApiClientFunctionalTest extends TestCase
{
    private static KubernetesApiClientFacade $apiClient;
    private static EventsApiClient $eventsApiClient;

    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();

        $logger = new Logger('test');

        $clientFacade = (new GenericClientFacadeFactory(
            (new RetryProxyFactory($logger))->createRetryProxy(),
            $logger
        ))->createClusterClient(
            (string) getenv('K8S_HOST'),
            (string) getenv('K8S_TOKEN'),
            (string) getenv('K8S_CA_CERT_PATH'),
            (string) getenv('K8S_NAMESPACE'),
        );

        self::$apiClient = $clientFacade;
        self::$eventsApiClient = $clientFacade->events();

        // generate a few events to test below
        self::cleanupCluster();

        $clientFacade->pods()->create(new Pod([
            'metadata' => [
                'name' => 'test-pod',
                'labels' => [
                    'app' => 'EventsApiClientFunctionalTest',
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

    public static function tearDownAfterClass(): void
    {
        parent::tearDownAfterClass();
        self::cleanupCluster();
    }

    private static function cleanupCluster(): void
    {
        self::$apiClient->deleteAllMatching(
            new DeleteOptions(['gracePeriodSeconds' => 0]),
            ['labelSelector' => 'app=EventsApiClientFunctionalTest']
        );
    }

    public function testList(): void
    {
        $result = self::$eventsApiClient->list();
        self::assertGreaterThan(0, count($result->items));
    }

    public function testListWithQuery(): void
    {
        //@TODO move logic to BaseNamespaceApiClientTestCase
        $result = self::$eventsApiClient->list([
            'fieldSelector' => 'involvedObject.name=test-pod',
        ]);

        self::assertGreaterThan(0, count($result->items));
        foreach ($result->items as $event) {
            self::assertInstanceOf(Event::class, $event);
            self::assertSame('test-pod', $event->involvedObject->name);
        }
    }

    public function testListForAllNamespaces(): void
    {
        $result = self::$eventsApiClient->listForAllNamespaces();
        self::assertGreaterThan(0, count($result->items));
    }
}
