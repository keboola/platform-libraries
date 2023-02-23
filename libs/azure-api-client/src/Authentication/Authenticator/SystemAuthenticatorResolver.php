<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator;

use Keboola\AzureApiClient\Authentication\AuthenticationToken;
use Keboola\AzureApiClient\Authentication\AuthorizationHeaderResolverInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class SystemAuthenticatorResolver implements AuthenticatorInterface
{
    private LoggerInterface $logger;

    /** @var callable|null */
    private $requestHandler;

    private ?AuthenticatorInterface $resolvedAuthenticator = null;

    /** @var callable|null */
    private $retryMiddleware;

    public function __construct(
        null|callable $retryMiddleware = null,
        null|callable $requestHandler = null,
        null|LoggerInterface $logger = null,
    ) {
        $this->retryMiddleware = $retryMiddleware;
        $this->requestHandler = $requestHandler;
        $this->logger = $logger ?? new NullLogger();
    }

    public function getAuthenticationToken(string $resource): AuthenticationToken
    {
        $this->resolvedAuthenticator ??= $this->resolveAuthenticator();
        return $this->resolvedAuthenticator->getAuthenticationToken($resource);
    }

    private function resolveAuthenticator(): AuthenticatorInterface
    {
        $logger = $this->options['logger'] ?? new NullLogger();

        $tenantId = (string) getenv('AZURE_TENANT_ID');
        $clientId = (string) getenv('AZURE_CLIENT_ID');
        $clientSecret = (string) getenv('AZURE_CLIENT_SECRET');
        if ($tenantId !== '' && $clientId !== '' && $clientSecret !== '') {
            $logger->debug('Found Azure client credentials in ENV, using ClientCredentialsAuthenticator');

            return new ClientCredentialsAuthenticator(
                tenantId: $tenantId,
                clientId: $clientId,
                clientSecret: $clientSecret,
                retryMiddleware: $this->retryMiddleware,
                requestHandler: $this->requestHandler,
                logger: $this->logger
            );
        }

        $logger->debug('Azure client credentials not found in ENV, using ManagedCredentialsAuthenticator');
        return new ManagedCredentialsAuthenticator(
            retryMiddleware: $this->retryMiddleware,
            requestHandler: $this->requestHandler,
            logger: $this->logger
        );
    }

    public function getHeaderResolver(string $resource): AuthorizationHeaderResolverInterface
    {
        $this->resolvedAuthenticator ??= $this->resolveAuthenticator();
        return $this->resolvedAuthenticator->getHeaderResolver($resource);
    }
}
