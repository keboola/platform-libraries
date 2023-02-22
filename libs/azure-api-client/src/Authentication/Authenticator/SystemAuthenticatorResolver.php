<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator;

use Keboola\AzureApiClient\Authentication\AuthenticationToken;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class SystemAuthenticatorResolver implements AuthenticatorInterface
{
    private ?AuthenticatorInterface $resolvedAuthenticator = null;

    /**
     * @param array{
     *     backoffMaxTries?: null|int<0, max>,
     *     requestHandler?: null|callable,
     *     logger?: null|LoggerInterface,
     * } $options
     */
    public function __construct(
        private readonly array $options = [],
    ) {
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
                $tenantId,
                $clientId,
                $clientSecret,
                $this->options,
            );
        }

        $logger->debug('Azure client credentials not found in ENV, using ManagedCredentialsAuthenticator');
        return new ManagedCredentialsAuthenticator($this->options);
    }
}
