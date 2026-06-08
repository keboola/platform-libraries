<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\StorageApiToken;

use Keboola\ApiBundle\Attribute\AuthAttributeInterface;
use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
use Keboola\ApiBundle\Security\TokenAuthenticatorInterface;
use Keboola\ApiBundle\Security\TokenInterface;
use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\ManageApi\ClientException as ManageApiClientException;
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

    public function __construct(
        private readonly StorageApiTokenFactory $tokenFactory,
        private readonly ?ManageApiClient $resolverClient = null,
    ) {
    }

    public function extractToken(Request $request): ?string
    {
        // Check Authorization header first
        $authHeader = $request->headers->get('Authorization');
        if ($authHeader !== null) {
            // Validate it's a Bearer token and strip prefix
            if (preg_match('/^Bearer\s+(.+)$/i', $authHeader, $matches)) {
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

        if ($this->resolverClient !== null && ProgrammaticToken::matches($token)) {
            return $this->exchangeProgrammaticToken($request, $token);
        }

        return $this->tokenFactory->createFromRequest($request);
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
        } catch (ManageApiClientException $e) {
            throw $this->mapResolverError($e);
        } catch (RuntimeException $e) {
            // Guzzle ConnectException or the ServiceAccount token file being
            // missing/unreadable/empty - a deployment/identity problem on our side.
            throw new CustomUserMessageAuthenticationException(
                'Token exchange is temporarily unavailable.',
                [],
                Response::HTTP_BAD_GATEWAY,
                $e,
            );
        }

        $storageToken = $resolved['storageToken'] ?? null;
        if (!is_string($storageToken) || $storageToken === '') {
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
        if ($rawProjectId === null || !ctype_digit($rawProjectId) || (int) $rawProjectId <= 0) {
            throw new CustomUserMessageAuthenticationException(
                sprintf('Missing or invalid "%s" header required for programmatic tokens.', self::PROJECT_ID_HEADER),
                [],
                Response::HTTP_BAD_REQUEST,
            );
        }

        return (int) $rawProjectId;
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
