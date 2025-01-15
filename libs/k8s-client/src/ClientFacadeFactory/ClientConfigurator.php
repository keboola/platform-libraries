<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFacadeFactory;

use GuzzleHttp\HandlerStack;
use Keboola\K8sClient\ClientFacadeFactory\Token\TokenInterface;
use Keboola\K8sClient\Exception\ConfigurationException;
use Keboola\K8sClient\Guzzle\AuthMiddleware;
use KubernetesRuntime\Client;

class ClientConfigurator
{
    public static function configureBaseClient(string $apiUrl, string $caCertFile, TokenInterface|string $token): void
    {
        if (!is_file($caCertFile) || !is_readable($caCertFile)) {
            throw new ConfigurationException(sprintf(
                'Invalid K8S CA cert path "%s". File does not exist or can\'t be read.',
                $caCertFile,
            ));
        }

        if (is_string($token)) {
            $token = new Token\StaticToken($token);
        }

        $guzzleHandler = HandlerStack::create();
        $guzzleHandler->push(new AuthMiddleware($token), 'auth');

        Client::configure(
            $apiUrl,
            [
                'caCert' => $caCertFile,
            ],
            [
                'handler' => $guzzleHandler,
                'connect_timeout' => '30',
                'timeout' => '60',
            ],
        );

        // the Client registers custom logging middleware to handler which we don't want to use
        // the middleware are the only two items in the handler stack without a name
        $guzzleHandler->remove('');
    }
}
