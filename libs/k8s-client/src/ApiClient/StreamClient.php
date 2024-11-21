<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use GuzzleHttp;
use KubernetesRuntime\Authentication;
use KubernetesRuntime\Exception\CommonException;
use KubernetesRuntime\Guzzle\Middleware;
use KubernetesRuntime\Logger;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LogLevel;

class StreamClient
{
    private static ?StreamClient $instance = null;

    /**
     * @var mixed[]
     */
    private array $defaultOptions = [];

    protected GuzzleHttp\Client $guzzle;

    protected string $caCert;
    protected string $clientCert;
    protected string $clientKey;
    protected string $token;
    protected string $username;
    protected string $password;
    protected string $master;

    /**
     * @param array<string, string>|Authentication $authenticationInfo
     * @param array<string, mixed> $guzzleOptions
     */
    private function __construct(string $master, array|Authentication $authenticationInfo, array $guzzleOptions = [])
    {
        // Setup basic Info
        $defaultOptions = [
            'base_uri'    => $master,
            'headers'     => [
                'Accepts' => 'application/json',
            ],
            'http_errors' => false,
        ];

        // Setup authentication
        if (!$authenticationInfo instanceof Authentication) {
            $authenticationInfo = new Authentication($authenticationInfo);
        }
        if ($authenticationInfo->caCert) {
            $defaultOptions['verify'] = $authenticationInfo->caCert;
        }
        if ($authenticationInfo->clientCert) {
            $defaultOptions['cert'] = $authenticationInfo->clientCert;
        }
        if ($authenticationInfo->clientKey) {
            $defaultOptions['ssl_key'] = $authenticationInfo->clientKey;
        }
        if ($authenticationInfo->token) {
            $defaultOptions['headers']['Authorization'] = 'Bearer ' . $authenticationInfo->token;
        }
        if ($authenticationInfo->username && $authenticationInfo->password) {
            $defaultOptions['auth'] = [
                $authenticationInfo->username,
                $authenticationInfo->password,
            ];
        }

        // Append to Handler Stack given, or create a new one.
        /** @var GuzzleHttp\HandlerStack $HandlerStack */
        $HandlerStack = $guzzleOptions['handler'] ?? GuzzleHttp\HandlerStack::create();
        // Setup logging bit
        $HandlerStack->push(
            GuzzleHttp\Middleware::log(
                Logger::getInstance()->getLogger(),
                new GuzzleHttp\MessageFormatter('{method} : {uri} - Request: {req_body}'),
                LogLevel::DEBUG,
            ),
        );

        // Set proper content-type for 'Patch' operation
        $HandlerStack->push(Middleware::setPatchOperation(), 'setPatchOperationContentType');

        $defaultOptions['handler'] = $HandlerStack;

        $this->defaultOptions = array_merge($defaultOptions, $guzzleOptions);

        // Create the actual client
        $this->guzzle = new GuzzleHttp\Client($this->defaultOptions);
    }

    /**
     * @param array<string, string>|Authentication $authenticationInfo
     * @param array<string, mixed> $guzzleOptions
     */
    public static function configure(
        string $master,
        array|Authentication $authenticationInfo,
        array $guzzleOptions = [],
    ): StreamClient {
        self::$instance = new StreamClient($master, $authenticationInfo, $guzzleOptions);

        return self::$instance;
    }

    public static function getInstance(): StreamClient
    {
        if (self::$instance instanceof StreamClient) {
            return self::$instance;
        } else {
            throw new CommonException('Must run StreamClient::configure() first!');
        }
    }

    /**
     * @param mixed[] $options
     */
    public function request(string $method, string $uri, array $options = []): ResponseInterface
    {
        $json    = null;
        $options = array_merge($this->defaultOptions, $options);

        return $this->guzzle->request($method, $uri, $options);
    }

    public function setDefaultOption(string $option, mixed $value): StreamClient
    {
        $this->defaultOptions[$option] = $value;

        return $this;
    }
}
