<?php

declare(strict_types=1);

namespace Keboola\K8sClient;

use Keboola\K8sClient\Exception\KubernetesResponseException;
use Keboola\K8sClient\Exception\ResourceAlreadyExistsException;
use Keboola\K8sClient\Exception\ResourceNotFoundException;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use KubernetesRuntime\AbstractAPI;
use Retry\RetryProxy;

/**
 * Low-level, namespaced K8S API client with integrated retries. Produced by a
 * {@see \Keboola\K8sClient\ClientFactory\KubernetesApiClientFactory} and handed to
 * {@see KubernetesApiClientFacade::create()} (and to any consumer-defined CRD clients).
 */
class KubernetesApiClient
{
    private const RAW_RESULT_PREVIEW_LENGTH = 512;

    private RetryProxy $retryProxy;
    protected string $k8sNamespace;

    public function __construct(RetryProxy $retryProxy, string $k8sNamespace)
    {
        $this->retryProxy = $retryProxy;
        $this->k8sNamespace = $k8sNamespace;
    }

    public function getK8sNamespace(): string
    {
        return $this->k8sNamespace;
    }

    /**
     * Run request on namespace-scoped API.
     *
     * Calls $api->{method}($this->k8sNamespace, ...$args)
     *
     * @param class-string<TResult> $expectedResult
     * @param mixed ...$args
     * @return TResult
     * @template TResult
     */
    public function request(AbstractAPI $api, string $method, string $expectedResult, ...$args)
    {
        return $this->clusterRequest($api, $method, $expectedResult, $this->k8sNamespace, ...$args);
    }

    /**
     * Run request on cluster-wide API.
     *
     * Calls $api->{method}(...$args)
     *
     * @param class-string<TResult> $expectedResult
     * @param mixed ...$args
     * @return TResult
     * @template TResult
     */
    public function clusterRequest(AbstractAPI $api, string $method, string $expectedResult, ...$args)
    {
        // network errors (connection failure, timeout etc.), server-side errors (5xx status) should be covered by retry
        // client errors (4xx status) should not be retried

        // inside the $api Guzzle is configured to not throw exception for 4xx/5xx status, it has to be handled manually
        // https://github.com/allansun/kubernetes-php-runtime/blob/f4d9466c1c8edc62c1f756f2812ceeb77f62b196/src/Client.php#L58

        $result = $this->retryProxy->call(function () use ($api, $method, $args) {
            $result = $api->{$method}(...$args);

            if ($result instanceof Status && $result->code >= 500) {
                throw new KubernetesResponseException(
                    sprintf('K8S request has failed: %s', $result->message),
                    $result,
                );
            }

            return $result;
        });

        if ($result instanceof Status && $result->status === 'Failure') {
            if ($result->code === 404) {
                throw new ResourceNotFoundException(
                    sprintf('Resource not found: %s', $result->message),
                    $result,
                );
            }

            if ($method === 'create' && $result->reason === 'AlreadyExists') {
                throw new ResourceAlreadyExistsException(
                    sprintf('Resource already exists: %s', $result->message),
                    $result,
                );
            }

            throw new KubernetesResponseException(
                sprintf('K8S request has failed: %s', $result->message),
                $result,
            );
        }

        if ($result instanceof $expectedResult) {
            return $result;
        }

        throw new KubernetesResponseException(
            sprintf(
                'Expected response class %s for request %s::%s, found %s',
                $expectedResult,
                get_class($api),
                $method,
                self::describeResult($result),
            ),
            null,
        );
    }

    /**
     * The runtime hands back the raw body of anything it can not map onto a K8S model and drops the HTTP status
     * code with it, so for those the body is the only thing left to say what the API server actually answered.
     */
    private static function describeResult(mixed $result): string
    {
        $type = get_debug_type($result);
        if (is_object($result)) {
            return $type;
        }

        $body = is_string($result) ? $result : json_encode($result);
        if (!is_string($body)) {
            return $type;
        }

        if (strlen($body) > self::RAW_RESULT_PREVIEW_LENGTH) {
            $body = substr($body, 0, self::RAW_RESULT_PREVIEW_LENGTH) . '...';
        }

        return sprintf('%s: "%s"', $type, $body);
    }
}
