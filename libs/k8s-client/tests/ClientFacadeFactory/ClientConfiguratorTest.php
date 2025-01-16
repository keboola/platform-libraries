<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ClientFacadeFactory;

use Keboola\K8sClient\ClientFacadeFactory\ClientConfigurator;
use Keboola\K8sClient\Exception\ConfigurationException;
use KubernetesRuntime\Client;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\ResponseInterface;

class ClientConfiguratorTest extends TestCase
{
    public function testConfigureBaseClient(): void
    {
        $namespace = (string) getenv('K8S_NAMESPACE');
        ClientConfigurator::configureBaseClient(
            apiUrl: (string) getenv('K8S_HOST'),
            caCertFile: (string) getenv('K8S_CA_CERT_PATH'),
            token: (string) getenv('K8S_TOKEN'),
        );

        $response = Client::getInstance()->request('GET', sprintf('/api/v1/namespaces/%s/pods', $namespace));
        self::assertInstanceOf(ResponseInterface::class, $response);
        self::assertSame(200, $response->getStatusCode());
        self::assertSame('application/json', $response->getHeaderLine('Content-Type'));

        $responseBody = json_decode($response->getBody()->getContents(), true);
        self::assertIsArray($responseBody);
        self::assertSame('PodList', $responseBody['kind']);
    }

    public function testClientDoesNotBreakStreamResponse(): void
    {
        $namespace = (string) getenv('K8S_NAMESPACE');
        ClientConfigurator::configureBaseClient(
            apiUrl: (string) getenv('K8S_HOST'),
            caCertFile: (string) getenv('K8S_CA_CERT_PATH'),
            token: (string) getenv('K8S_TOKEN'),
        );

        $response = Client::getInstance()->request('GET', sprintf('/api/v1/namespaces/%s/pods', $namespace));
        self::assertInstanceOf(ResponseInterface::class, $response);
        self::assertSame(200, $response->getStatusCode());
        self::assertSame('application/json', $response->getHeaderLine('Content-Type'));

        // check the response is pointing to the beginning of the stream
        self::assertSame(0, $response->getBody()->tell());
    }

    public function testInvalidCaFile(): void
    {
        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage('Invalid K8S CA cert path "/foo". File does not exist or can\'t be read.');

        ClientConfigurator::configureBaseClient(
            apiUrl: (string) getenv('K8S_HOST'),
            caCertFile: '/foo',
            token: (string) getenv('K8S_TOKEN'),
        );
    }
}
