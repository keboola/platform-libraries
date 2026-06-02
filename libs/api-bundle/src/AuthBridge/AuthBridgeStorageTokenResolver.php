<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\AuthBridge;

use Closure;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use JsonException;
use Keboola\ApiBundle\AuthBridge\Exception\InvalidResolverRequestException;
use Keboola\ApiBundle\AuthBridge\Exception\ProjectAccessDeniedException;
use Keboola\ApiBundle\AuthBridge\Exception\ResolverUnavailableException;
use Keboola\ApiBundle\AuthBridge\Exception\StorageTokenResolverException;
use Keboola\ApiBundle\AuthBridge\Exception\UnauthorizedSubjectTokenException;
use Keboola\ServiceClient\ServiceClient;
use Keboola\ServiceClient\ServiceDnsType;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use SensitiveParameter;

/**
 * Guzzle implementation of {@see StorageTokenResolverInterface} that calls Connection's
 * internal auth-bridge resolver endpoint.
 *
 * The service authenticates itself with its projected Kubernetes ServiceAccount JWT
 * (X-Kubernetes-Authorization) and hands over the user's programmatic token
 * (X-Subject-Token). The SA token is read inside {@see resolve()} on every call (so
 * kubelet-rotated tokens are picked up) and set explicitly on the request — no auth
 * middleware — so the token is fully under our control and is never logged.
 *
 * Error messages are deliberately fixed and never echo the Connection response body, which
 * could otherwise leak subject-token or storage-token material.
 */
class AuthBridgeStorageTokenResolver implements StorageTokenResolverInterface
{
    private const RESOLVE_PATH = 'manage/internal/auth-bridge/resolve-storage-token';
    private const SUBJECT_TOKEN_HEADER = 'X-Subject-Token';
    private const SERVICE_ACCOUNT_HEADER = 'X-Kubernetes-Authorization';
    private const USER_AGENT = 'Keboola Auth Bridge Storage Token Resolver';

    private readonly GuzzleClient $httpClient;

    public function __construct(
        private readonly ServiceClient $serviceClient,
        private readonly KubernetesServiceAccountTokenProvider $serviceAccountTokenProvider,
        ServiceDnsType $connectionDnsType = ServiceDnsType::INTERNAL,
        ?string $userAgent = null,
        LoggerInterface $logger = new NullLogger(),
        null|Closure|HandlerStack $requestHandler = null,
    ) {
        $stack = $requestHandler instanceof HandlerStack
            ? $requestHandler
            : HandlerStack::create($requestHandler);

        $stack->push(Middleware::log(
            $logger,
            new MessageFormatter('[auth-bridge-resolver] {method} {uri} : {code}'),
        ));

        $resolvedUserAgent = self::USER_AGENT;
        if ($userAgent !== null) {
            $resolvedUserAgent .= ' - ' . $userAgent;
        }

        $this->httpClient = new GuzzleClient([
            'base_uri' => rtrim($this->serviceClient->getConnectionServiceUrl($connectionDnsType), '/') . '/',
            'handler' => $stack,
            'headers' => [
                'User-Agent' => $resolvedUserAgent,
                'Content-Type' => 'application/json',
            ],
            'connect_timeout' => 10,
            'timeout' => 30,
        ]);
    }

    public function resolve(int $projectId, #[SensitiveParameter] string $subjectToken): ResolvedStorageToken
    {
        $serviceAccountToken = $this->serviceAccountTokenProvider->getToken();

        try {
            $response = $this->httpClient->post(self::RESOLVE_PATH, [
                'headers' => [
                    self::SERVICE_ACCOUNT_HEADER => 'Bearer ' . $serviceAccountToken,
                    self::SUBJECT_TOKEN_HEADER => 'Bearer ' . $subjectToken,
                ],
                'json' => [
                    'projectId' => $projectId,
                ],
            ]);

            $data = json_decode($response->getBody()->getContents(), true, 512, JSON_THROW_ON_ERROR);
            if (!is_array($data)) {
                throw new StorageTokenResolverException('Auth bridge resolver returned an invalid response.');
            }

            return ResolvedStorageToken::fromResponseData($data);
        } catch (ConnectException $e) {
            throw new ResolverUnavailableException('Auth bridge resolver is unreachable.', 0, $e);
        } catch (RequestException $e) {
            $response = $e->getResponse();
            if ($response === null) {
                throw new ResolverUnavailableException('Auth bridge resolver is unreachable.', 0, $e);
            }

            throw match ($response->getStatusCode()) {
                400 => new InvalidResolverRequestException('Auth bridge resolver rejected the request.', 0, $e),
                401 => new UnauthorizedSubjectTokenException(
                    'Subject token was rejected by the auth bridge resolver.',
                    0,
                    $e,
                ),
                403 => new ProjectAccessDeniedException(
                    'Subject token cannot access the requested project.',
                    0,
                    $e,
                ),
                default => $response->getStatusCode() >= 500
                    ? new ResolverUnavailableException('Auth bridge resolver returned a server error.', 0, $e)
                    : new StorageTokenResolverException(
                        'Auth bridge resolver returned an unexpected error.',
                        0,
                        $e,
                    ),
            };
        } catch (JsonException $e) {
            throw new StorageTokenResolverException('Auth bridge resolver returned invalid JSON.', 0, $e);
        } catch (GuzzleException $e) {
            throw new ResolverUnavailableException('Auth bridge resolver call failed.', 0, $e);
        }
    }
}
