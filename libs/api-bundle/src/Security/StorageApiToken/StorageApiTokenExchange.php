<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\StorageApiToken;

use Keboola\ApiBundle\AuthBridge\Exception\InvalidResolverRequestException;
use Keboola\ApiBundle\AuthBridge\Exception\ProjectAccessDeniedException;
use Keboola\ApiBundle\AuthBridge\Exception\ResolverUnavailableException;
use Keboola\ApiBundle\AuthBridge\Exception\StorageTokenResolverException;
use Keboola\ApiBundle\AuthBridge\Exception\UnauthorizedSubjectTokenException;
use Keboola\ApiBundle\AuthBridge\StorageTokenResolverInterface;
use SensitiveParameter;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;

/**
 * Exchanges a Connection programmatic token (kbc_at_* / kbc_pat_*) for a {@see StorageApiToken}:
 * reads the project id from the request, resolves the legacy Storage token through Connection,
 * and verifies it. Resolver failures are translated to authentication exceptions whose HTTP code
 * is surfaced to the client. Shared by both the transparent and the explicit authenticators.
 */
class StorageApiTokenExchange
{
    public function __construct(
        private readonly StorageTokenResolverInterface $resolver,
        private readonly StorageApiTokenFactory $tokenFactory,
    ) {
    }

    public function exchange(
        Request $request,
        #[SensitiveParameter]
        string $subjectToken,
        string $projectIdHeader,
    ): StorageApiToken {
        $projectId = $this->extractProjectId($request, $projectIdHeader);

        try {
            $resolvedToken = $this->resolver->resolve($projectId, $subjectToken);
        } catch (UnauthorizedSubjectTokenException $e) {
            throw new CustomUserMessageAuthenticationException(
                'Invalid authentication token.',
                [],
                Response::HTTP_UNAUTHORIZED,
                $e,
            );
        } catch (ProjectAccessDeniedException $e) {
            throw new CustomUserMessageAuthenticationException(
                'Authentication token is not allowed to access the project.',
                [],
                Response::HTTP_FORBIDDEN,
                $e,
            );
        } catch (InvalidResolverRequestException $e) {
            throw new CustomUserMessageAuthenticationException(
                'Invalid token exchange request.',
                [],
                Response::HTTP_BAD_REQUEST,
                $e,
            );
        } catch (ResolverUnavailableException $e) {
            throw new CustomUserMessageAuthenticationException(
                'Token exchange is temporarily unavailable.',
                [],
                Response::HTTP_BAD_GATEWAY,
                $e,
            );
        } catch (StorageTokenResolverException $e) {
            throw new CustomUserMessageAuthenticationException(
                'Token exchange failed.',
                [],
                Response::HTTP_BAD_GATEWAY,
                $e,
            );
        }

        return $this->tokenFactory->createFromResolvedToken($request, $resolvedToken->storageToken);
    }

    private function extractProjectId(Request $request, string $projectIdHeader): int
    {
        $rawProjectId = $request->headers->get($projectIdHeader);
        if ($rawProjectId === null || !ctype_digit($rawProjectId) || (int) $rawProjectId <= 0) {
            throw new CustomUserMessageAuthenticationException(
                sprintf('Missing or invalid "%s" header required for programmatic tokens.', $projectIdHeader),
                [],
                Response::HTTP_BAD_REQUEST,
            );
        }

        return (int) $rawProjectId;
    }
}
