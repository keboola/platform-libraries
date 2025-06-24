<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Util;

use KubernetesRuntime\AbstractModel;
use Psr\Http\Message\ResponseInterface;
use RuntimeException;

class StreamResponse
{
    /**
     * @return (null|string)[]
     */
    public static function chunkStreamResponse(ResponseInterface $response, int $readWaitTimeout): iterable
    {
        $contentType = $response->getHeaderLine('Content-Type');
        if ($contentType !== 'application/json') {
            throw new RuntimeException(sprintf('Can\'t process response with content type "%s"', $contentType));
        }

        $rawStream = $response->getBody()->detach();
        if ($rawStream === null) {
            return [];
        }

        try {
            yield from self::chunkJsonStream($rawStream, $readWaitTimeout);
        } finally {
            fclose($rawStream);
        }
    }

    /**
     * @param resource $stream
     * @return (null|string)[]
     */
    private static function chunkJsonStream($stream, int $readWaitTimeout): iterable
    {
        if ($readWaitTimeout > 0) {
            stream_set_timeout($stream, $readWaitTimeout);
        }

        $buffer = '';
        while (!feof($stream)) {
            $chunk = fgets($stream, 4096);
            if ($chunk === false) {
                if (stream_get_meta_data($stream)['timed_out']) {
                    yield null;
                    continue;
                }

                throw new RuntimeException('Failed to read from response stream');
            }

            $buffer .= $chunk;
            if (str_ends_with($buffer, "\n")) {
                $buffer = trim($buffer);
                if ($buffer !== '') {
                    yield $buffer;
                }

                $buffer = '';
            }
        }

        if (str_ends_with($buffer, "\n")) {
            $buffer = trim($buffer);
            if ($buffer !== '') {
                yield $buffer;
            }
        }
    }

    public static function instantiateResponseObject(array $data): AbstractModel
    {
        $apiVersion = $data['apiVersion'] ?? null;
        $kind = $data['kind'] ?? null;

        if ($apiVersion === null || $kind === null) {
            throw new RuntimeException('Missing apiVersion or kind in object');
        }

        $class = self::resolveClassForKind($apiVersion, $kind);

        return new $class($data);
    }

    /**
     * @return class-string<AbstractModel>
     */
    public static function resolveClassForKind(string $apiVersion, string $kind): string
    {
        // apiVersion: v1                   -> api: core.k8s.io, apiVersion: v1
        // apiVersion: apps/v1              -> api: apps.k8s.io, apiVersion: v1
        // apiVersion: networking.k8s.io/v1 -> api: networking.k8s.io, apiVersion: v1
        if (!str_contains($apiVersion, '/')) {
            $api = 'core.k8s.io';
        } else {
            [$api, $apiVersion] = explode('/', $apiVersion, 2);

            if (!str_contains($api, '.')) {
                $api = $api . '.k8s.io';
            }
        }

        // api: core.k8s.io, apiVersion: v1, kind: Pod
        // result: Kubernetes\Model\Io\K8s\Api\Core\V1\Pod
        $apiParts = explode('.', $api);
        $apiParts = array_reverse($apiParts);
        array_splice($apiParts, -1, 0, ['Api']);
        array_push($apiParts, $apiVersion);
        array_push($apiParts, $kind);
        $apiParts = array_map(ucfirst(...), $apiParts);

        $class = 'Kubernetes\\Model\\' . implode('\\', $apiParts);
        if (!class_exists($class)) {
            throw new RuntimeException(sprintf('Class "%s" does not exist', $class));
        }

        if (!is_subclass_of($class, AbstractModel::class)) {
            throw new RuntimeException(sprintf('Class "%s" is not "%s"', $class, AbstractModel::class));
        }

        return $class;
    }
}
