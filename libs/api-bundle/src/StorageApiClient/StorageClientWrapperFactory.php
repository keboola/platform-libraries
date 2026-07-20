<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\StorageApiClient;

use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\AuthType;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use SensitiveParameter;
use Symfony\Component\HttpFoundation\Request;

/**
 * Single place that turns base {@see ClientOptions} plus a resolved (token, authType) into a
 * Storage {@see ClientWrapper}: it clones the base (so it is never mutated), merges optional
 * per-call overrides, pins the token/authType (which therefore win over overrides) and resolves
 * the run id from the request. Shared by the controller-facing {@see StorageClientApiFactory} and
 * the token-verification {@see RequestStorageClientFactory} so the wrapper is built one way only.
 */
final class StorageClientWrapperFactory
{
    public const RUN_ID_HEADER = 'X-KBC-RunId';

    public static function create(
        ClientOptions $baseClientOptions,
        #[SensitiveParameter]
        string $token,
        AuthType $authType,
        Request $request,
        ?ClientOptions $overrides = null,
    ): ClientWrapper {
        $options = clone $baseClientOptions;
        if ($overrides !== null) {
            $options->addValuesFrom($overrides);
        }

        $options->setToken($token);
        $options->setAuthType($authType);
        $options->setRunId(self::resolveRunId($request, $options));

        return new ClientWrapper($options);
    }

    private static function resolveRunId(Request $request, ClientOptions $options): string
    {
        $runId = (string) $request->headers->get(self::RUN_ID_HEADER);
        if ($runId !== '') {
            return $runId;
        }

        $runIdGenerator = $options->getRunIdGenerator();
        if ($runIdGenerator !== null) {
            $runId = $runIdGenerator($options);
            assert(is_string($runId));
            return $runId;
        }

        return uniqid('run-');
    }
}
