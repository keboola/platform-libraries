<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\StorageApiToken;

use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\ManageApi\ClientException as ManageApiClientException;
use Keboola\ManageApi\MaintenanceException;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\MaintenanceException as StorageApiMaintenanceException;
use Keboola\StorageApiBranch\Factory\StorageClientRequestFactory;
use Psr\Log\LoggerInterface;
use RuntimeException;
use SensitiveParameter;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;

/**
 * Builds a {@see StorageApiToken}. Legacy tokens carried by the request are verified directly
 * against Storage API ({@see self::createFromRequest()}). Connection programmatic tokens
 * (kbc_at_* / kbc_pat_*) are first exchanged for a legacy Storage token through Manage API's
 * auth-bridge resolver ({@see ManageApiClient::resolveStorageToken()}), then verified the same
 * way ({@see self::createFromProgrammaticToken()}).
 *
 * The resolver client authenticates with the service's projected Kubernetes ServiceAccount JWT
 * (read per request, so kubelet-rotated tokens are picked up).
 */
class StorageApiTokenFactory
{
    private const PROJECT_ID_HEADER = 'X-KBC-ProjectId';

    public function __construct(
        private readonly StorageClientRequestFactory $clientRequestFactory,
        private readonly ManageApiClient $resolverClient,
        private readonly LoggerInterface $logger,
    ) {
    }

    /**
     * Verifies the token carried by the request (Authorization: Bearer or X-StorageApi-Token).
     */
    public function createFromRequest(Request $request): StorageApiToken
    {
        try {
            return $this->verifyRequestToken($request);
        } catch (ClientException $e) {
            if ($e->getCode() >= 400 && $e->getCode() < 500) {
                throw new CustomUserMessageAuthenticationException($e->getMessage(), [], $e->getCode(), $e);
            }
            throw $e;
        }
    }

    /**
     * Resolves a programmatic token to a legacy Storage token via Manage API and verifies it.
     * Resolver failures are translated to authentication exceptions whose HTTP code is surfaced to
     * the client; error messages are fixed and never echo the Connection/Manage response body,
     * which could otherwise leak subject-token or storage-token material.
     */
    public function createFromProgrammaticToken(
        Request $request,
        #[SensitiveParameter]
        string $subjectToken,
    ): StorageApiToken {
        $projectId = $this->extractProjectId($request);

        try {
            // Treated as untyped here on purpose: this is an HTTP boundary, so the response shape
            // is validated below rather than trusted from the client's declared return type.
            /** @var array<string, mixed> $resolved */
            $resolved = $this->resolverClient->resolveStorageToken($projectId, $subjectToken);
        } catch (MaintenanceException $e) {
            // Connection is in maintenance. Surface it as 503 (must be caught before the generic
            // ManageApiClientException - MaintenanceException is a subclass) so the client can retry
            // instead of treating it as a generic upstream failure.
            $this->logger->warning('Storage token exchange unavailable: Connection is in maintenance.', [
                'projectId' => $projectId,
                'retryAfter' => $e->getRetryAfter(),
            ]);
            throw new CustomUserMessageAuthenticationException(
                'Token exchange is temporarily unavailable.',
                [],
                Response::HTTP_SERVICE_UNAVAILABLE,
                $e,
            );
        } catch (ManageApiClientException $e) {
            $mapped = $this->mapResolverError($e);
            // 5xx / unexpected statuses are our-side incidents (deploy / identity / Connection
            // outage). Log them for diagnosis, but never the Manage response body - it may carry
            // subject- or storage-token material.
            if ($mapped->getCode() >= Response::HTTP_INTERNAL_SERVER_ERROR) {
                $this->logger->warning('Storage token exchange failed: resolver returned an unexpected status.', [
                    'projectId' => $projectId,
                    'resolverStatus' => $e->getCode(),
                ]);
            }
            throw $mapped;
        } catch (RuntimeException $e) {
            // Guzzle ConnectException or the ServiceAccount token file being
            // missing/unreadable/empty - a deployment/identity problem on our side. The message is
            // a network/file error and carries no token material, so it is safe to log.
            $this->logger->warning('Storage token exchange unavailable: resolver call failed.', [
                'projectId' => $projectId,
                'reason' => $e->getMessage(),
            ]);
            throw new CustomUserMessageAuthenticationException(
                'Token exchange is temporarily unavailable.',
                [],
                Response::HTTP_BAD_GATEWAY,
                $e,
            );
        }

        $storageToken = $resolved['storageToken'] ?? null;
        if (!is_string($storageToken) || $storageToken === '') {
            // Never log $resolved itself - it may contain a storageToken value.
            $this->logger->warning(
                'Storage token exchange failed: resolver response did not contain a storage token.',
                ['projectId' => $projectId],
            );
            throw new CustomUserMessageAuthenticationException(
                'Token exchange is temporarily unavailable.',
                [],
                Response::HTTP_BAD_GATEWAY,
            );
        }

        try {
            return $this->createFromResolvedToken($request, $storageToken);
        } catch (StorageApiMaintenanceException $e) {
            // Storage went into maintenance between the resolver call and the verification.
            // Surface it as 503 (must be caught before the generic ClientException -
            // StorageApiMaintenanceException is a subclass) so the client can retry.
            $this->logger->warning('Storage token exchange unavailable: Storage API is in maintenance.', [
                'projectId' => $projectId,
                'retryAfter' => $e->getRetryAfter(),
            ]);
            throw new CustomUserMessageAuthenticationException(
                'Token exchange is temporarily unavailable.',
                [],
                Response::HTTP_SERVICE_UNAVAILABLE,
                $e,
            );
        } catch (ClientException $e) {
            // The resolved legacy token came from Connection, so a Storage rejection here (even a
            // 4xx) is an upstream/our-side incident, never the caller's fault. Don't echo the
            // Storage message - it could carry token material - and surface 502.
            $this->logger->warning('Storage token exchange failed: resolved token verification failed.', [
                'projectId' => $projectId,
                'storageStatus' => $e->getCode(),
            ]);
            throw new CustomUserMessageAuthenticationException(
                'Token exchange is temporarily unavailable.',
                [],
                Response::HTTP_BAD_GATEWAY,
                $e,
            );
        }
    }

    /**
     * Verifies a legacy Storage token resolved from a programmatic token. The resolved token is
     * placed on a copy of the request as {@see StorageClientRequestFactory::TOKEN_HEADER} and any
     * incoming Authorization header is dropped, so the Storage client uses the legacy token (not
     * the original bearer token). The original request is left untouched.
     */
    private function createFromResolvedToken(
        Request $request,
        #[SensitiveParameter]
        string $legacyStorageToken,
    ): StorageApiToken {
        $exchangedRequest = clone $request;
        $exchangedRequest->headers = clone $request->headers;
        $exchangedRequest->headers->remove('Authorization');
        $exchangedRequest->headers->set(StorageClientRequestFactory::TOKEN_HEADER, $legacyStorageToken);

        return $this->verifyRequestToken($exchangedRequest);
    }

    /**
     * Raw verification against Storage API, shared by both paths; callers map the exceptions
     * ({@see self::createFromRequest()} echoes 4xx messages for legacy tokens,
     * {@see self::createFromProgrammaticToken()} uses fixed messages for exchanged tokens).
     */
    private function verifyRequestToken(Request $request): StorageApiToken
    {
        $wrapper = $this->clientRequestFactory->createClientWrapper($request);
        $storageApiClient = $wrapper->getBasicClient();
        $tokenInfo = $storageApiClient->verifyToken();

        return new StorageApiToken($tokenInfo, $storageApiClient->getTokenString());
    }

    private function extractProjectId(Request $request): int
    {
        $rawProjectId = $request->headers->get(self::PROJECT_ID_HEADER);
        // FILTER_VALIDATE_INT rejects non-numeric input, values outside the int range (no silent
        // wrap-around past PHP_INT_MAX) and - via min_range - zero/negative ids.
        $projectId = $rawProjectId === null
            ? false
            : filter_var($rawProjectId, FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
        if ($projectId === false) {
            throw new CustomUserMessageAuthenticationException(
                sprintf('Missing or invalid "%s" header required for programmatic tokens.', self::PROJECT_ID_HEADER),
                [],
                Response::HTTP_BAD_REQUEST,
            );
        }

        return $projectId;
    }

    private function mapResolverError(ManageApiClientException $e): CustomUserMessageAuthenticationException
    {
        return match ($e->getCode()) {
            Response::HTTP_BAD_REQUEST => new CustomUserMessageAuthenticationException(
                'Invalid token exchange request.',
                [],
                Response::HTTP_BAD_REQUEST,
                $e,
            ),
            Response::HTTP_UNAUTHORIZED => new CustomUserMessageAuthenticationException(
                'Invalid authentication token.',
                [],
                Response::HTTP_UNAUTHORIZED,
                $e,
            ),
            Response::HTTP_FORBIDDEN => new CustomUserMessageAuthenticationException(
                'Authentication token is not allowed to access the project.',
                [],
                Response::HTTP_FORBIDDEN,
                $e,
            ),
            default => new CustomUserMessageAuthenticationException(
                'Token exchange is temporarily unavailable.',
                [],
                Response::HTTP_BAD_GATEWAY,
                $e,
            ),
        };
    }
}
