<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\AuthBridge;

use Keboola\ApiBundle\AuthBridge\Exception\InvalidResolverRequestException;
use Keboola\ApiBundle\AuthBridge\Exception\ProjectAccessDeniedException;
use Keboola\ApiBundle\AuthBridge\Exception\ResolverUnavailableException;
use Keboola\ApiBundle\AuthBridge\Exception\StorageTokenResolverException;
use Keboola\ApiBundle\AuthBridge\Exception\UnauthorizedSubjectTokenException;
use Keboola\ApiBundle\Security\ApplicationToken\ManageApiClientFactory;
use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\ManageApi\ClientException;
use Keboola\ServiceClient\ServiceDnsType;
use RuntimeException;
use SensitiveParameter;
use Symfony\Component\HttpFoundation\Response;

/**
 * {@see StorageTokenResolverInterface} backed by the Manage API client's
 * {@see ManageApiClient::resolveStorageToken()} (available since keboola/kbc-manage-api-php-client
 * v10.2). The client authenticates with the service's projected Kubernetes ServiceAccount JWT
 * (read per request, so kubelet-rotated tokens are picked up) and forwards the user's programmatic
 * token to Connection's internal auth-bridge resolver endpoint.
 *
 * Manage API failures are translated to the typed resolver exceptions consumed by
 * {@see \Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenExchange}. Error messages are
 * deliberately fixed and never echo the Connection response body, which could otherwise leak
 * subject-token or storage-token material.
 */
final class ManageApiStorageTokenResolver implements StorageTokenResolverInterface
{
    private readonly ManageApiClient $client;

    public function __construct(
        ManageApiClientFactory $clientFactory,
        string $serviceAccountTokenPath,
        ServiceDnsType $connectionDnsType = ServiceDnsType::INTERNAL,
    ) {
        $this->client = $clientFactory->getClientForServiceAccountTokenPath(
            $serviceAccountTokenPath,
            $connectionDnsType,
        );
    }

    public function resolve(int $projectId, #[SensitiveParameter] string $subjectToken): ResolvedStorageToken
    {
        try {
            $data = $this->client->resolveStorageToken($projectId, $subjectToken);
        } catch (ClientException $e) {
            throw $this->mapClientException($e);
        } catch (RuntimeException $e) {
            // The ServiceAccount token file is missing/unreadable/empty, or Connection is
            // unreachable (Guzzle ConnectException extends RuntimeException). Either way this is a
            // deployment/identity problem on our side, not a client fault.
            throw new ResolverUnavailableException('Auth bridge resolver is unreachable.', 0, $e);
        }

        return ResolvedStorageToken::fromResponseData($data);
    }

    private function mapClientException(ClientException $e): StorageTokenResolverException
    {
        $statusCode = $e->getCode();

        return match ($statusCode) {
            Response::HTTP_BAD_REQUEST => new InvalidResolverRequestException(
                'Auth bridge resolver rejected the request.',
                0,
                $e,
            ),
            Response::HTTP_UNAUTHORIZED => new UnauthorizedSubjectTokenException(
                'Subject token was rejected by the auth bridge resolver.',
                0,
                $e,
            ),
            Response::HTTP_FORBIDDEN => new ProjectAccessDeniedException(
                'Subject token cannot access the requested project.',
                0,
                $e,
            ),
            default => $statusCode >= Response::HTTP_INTERNAL_SERVER_ERROR || $statusCode === 0
                ? new ResolverUnavailableException('Auth bridge resolver returned a server error.', 0, $e)
                : new StorageTokenResolverException(
                    'Auth bridge resolver returned an unexpected error.',
                    0,
                    $e,
                ),
        };
    }
}
