<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\StorageApiClient;

use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\AuthType;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use SensitiveParameter;
use Symfony\Component\HttpFoundation\Request;

/**
 * The single place a Storage {@see ClientWrapper} is built in the bundle: it clones the base
 * {@see ClientOptions} (so the base is never mutated), merges optional per-call overrides, pins the
 * given token/authType (which therefore win over overrides) and resolves the run id from the request.
 *
 * The controller-facing {@see StorageClientApiFactory} delegates here with its bound token, and token
 * verification ({@see \Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenFactory}) uses it
 * with the raw request token. Kept injectable (non-final) so functional tests can replace it via
 * {@see \Keboola\ApiBundle\Test\AuthenticatorTestTrait}.
 *
 * @internal Bundle-internal wiring; consumers use {@see StorageClientApiFactory} (or #[StorageApiTokenAuth]).
 */
class StorageClientRequestFactory
{
    public const RUN_ID_HEADER = 'X-KBC-RunId';

    public function __construct(
        private readonly ClientOptions $baseClientOptions,
    ) {
    }

    public function createClientWrapper(
        #[SensitiveParameter]
        string $token,
        AuthType $authType,
        Request $request,
        ?ClientOptions $overrides = null,
    ): ClientWrapper {
        $options = clone $this->baseClientOptions;
        if ($overrides !== null) {
            $options->addValuesFrom($overrides);
        }

        $options->setToken($token);
        $options->setAuthType($authType);
        $options->setRunId($this->resolveRunId($request, $options));

        return new ClientWrapper($options);
    }

    private function resolveRunId(Request $request, ClientOptions $options): string
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
