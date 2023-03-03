<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator;

use Closure;
use Keboola\AzureApiClient\Authentication\AuthenticationToken;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class SystemAuthenticatorResolver implements AuthenticatorInterface
{
    private ?AuthenticatorInterface $resolvedAuthenticator = null;

    /**
     * @param int<0, max>|null $backoffMaxTries
     */
    public function __construct(
        private readonly ?int $backoffMaxTries = null,
        private readonly ?Closure $requestHandler = null,
        private readonly LoggerInterface $logger = new NullLogger(),
    ) {
    }

    public function getAuthenticationToken(string $resource): AuthenticationToken
    {
        $this->resolvedAuthenticator ??= $this->resolveAuthenticator();
        return $this->resolvedAuthenticator->getAuthenticationToken($resource);
    }

    private function resolveAuthenticator(): AuthenticatorInterface
    {
        $tenantId = (string) getenv('AZURE_TENANT_ID');
        $clientId = (string) getenv('AZURE_CLIENT_ID');
        $clientSecret = (string) getenv('AZURE_CLIENT_SECRET');
        if ($tenantId !== '' && $clientId !== '' && $clientSecret !== '') {
            $this->logger->debug('Found Azure client credentials in ENV, using ClientCredentialsAuthenticator');

            return new ClientCredentialsAuthenticator(
                $tenantId,
                $clientId,
                $clientSecret,
                backoffMaxTries: $this->backoffMaxTries,
                requestHandler: $this->requestHandler,
                logger: $this->logger,
            );
        }

        $this->logger->debug('Azure client credentials not found in ENV, using ManagedCredentialsAuthenticator');
        return new ManagedCredentialsAuthenticator(
            backoffMaxTries: $this->backoffMaxTries,
            requestHandler: $this->requestHandler,
            logger: $this->logger,
        );
    }
}
