<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\StorageApiToken;

use Keboola\ApiBundle\Attribute\AuthAttributeInterface;
use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
use Keboola\ApiBundle\Security\TokenAuthenticatorInterface;
use Keboola\ApiBundle\Security\TokenInterface;
use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\ManageApi\ClientException as ManageApiClientException;
use Keboola\ManageApi\MaintenanceException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use RuntimeException;
use SensitiveParameter;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;

/**
 * Authenticates a Storage API token. Legacy tokens (X-StorageApi-Token / Bearer) are verified
 * directly against Storage API. Connection programmatic tokens (kbc_at_* / kbc_pat_*) are first
 * exchanged for a legacy Storage token through Manage API's auth-bridge resolver
 * ({@see ManageApiClient::resolveStorageToken()}), then verified the same way — both paths yield a
 * {@see StorageApiToken}.
 *
 * The resolver client authenticates with the service's projected Kubernetes ServiceAccount JWT
 * (read per request, so kubelet-rotated tokens are picked up). When no resolver client is wired,
 * programmatic tokens fall through to the legacy verification (and fail there).
 *
 * @implements TokenAuthenticatorInterface<StorageApiToken>
 */
class StorageApiTokenAuthenticator implements TokenAuthenticatorInterface
{
    private const PROJECT_ID_HEADER = 'X-KBC-ProjectId';
    private const BEARER_PATTERN = '/^Bearer\s+(.+)$/i';

    private readonly LoggerInterface $logger;

    public function __construct(
        private readonly StorageApiTokenFactory $tokenFactory,
        private readonly ?ManageApiClient $resolverClient = null,
        ?LoggerInterface $logger = null,
    ) {
        $this->logger = $logger ?? new NullLogger();
    }

    public function extractToken(Request $request): ?string
    {
        // Check Authorization header first
        $authHeader = $request->headers->get('Authorization');
        if ($authHeader !== null) {
            // Validate it's a Bearer token and strip prefix
            if (preg_match(self::BEARER_PATTERN, $authHeader, $matches)) {
                return $matches[1];
            }
            return $authHeader;
        }

        // Check X-StorageApi-Token header
        return $request->headers->get('X-StorageApi-Token');
    }

    public function authenticateToken(
        AuthAttributeInterface $authAttribute,
        string $token,
        Request $request,
    ): StorageApiToken {
        assert($authAttribute instanceof StorageApiTokenAuth);

        // Exchange is restricted to programmatic tokens that explicitly arrive as
        // `Authorization: Bearer <kbc_at|kbc_pat>`. A bare `Authorization: <kbc_...>` or
        // `X-StorageApi-Token: kbc_...` is an undocumented shape and stays on the legacy
        // verification path, preserving pre-exchange behaviour.
        $programmaticToken = $this->extractProgrammaticBearerToken($request);
        if ($this->resolverClient !== null && $programmaticToken !== null) {
            return $this->exchangeProgrammaticToken($request, $programmaticToken);
        }

        return $this->tokenFactory->createFromRequest($request);
    }

    /**
     * Returns the programmatic token only when it is presented as `Authorization: Bearer <kbc_...>`,
     * otherwise null. Other carriers (bare Authorization value, X-StorageApi-Token) are not eligible
     * for exchange.
     */
    private function extractProgrammaticBearerToken(Request $request): ?string
    {
        $authHeader = $request->headers->get('Authorization');
        if ($authHeader === null || !preg_match(self::BEARER_PATTERN, $authHeader, $matches)) {
            return null;
        }

        $bearerToken = $matches[1];

        return ProgrammaticToken::matches($bearerToken) ? $bearerToken : null;
    }

    public function authorizeToken(AuthAttributeInterface $authAttribute, TokenInterface $token): void
    {
        assert($authAttribute instanceof StorageApiTokenAuth);
        assert($token instanceof StorageApiToken);

        $missingFeatures = array_diff($authAttribute->features, $token->getFeatures());
        if (count($missingFeatures) > 0) {
            throw new AccessDeniedException(sprintf(
                'Authentication token is valid but missing following features: %s',
                implode(', ', $missingFeatures),
            ));
        }
    }

    /**
     * Resolves a programmatic token to a legacy Storage token via Manage API and verifies it.
     * Resolver failures are translated to authentication exceptions whose HTTP code is surfaced to
     * the client; error messages are fixed and never echo the Connection/Manage response body,
     * which could otherwise leak subject-token or storage-token material.
     */
    private function exchangeProgrammaticToken(
        Request $request,
        #[SensitiveParameter]
        string $subjectToken,
    ): StorageApiToken {
        assert($this->resolverClient !== null);

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

        return $this->tokenFactory->createFromResolvedToken($request, $storageToken);
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
