<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\StorageApiClient;

use Closure;
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

    /**
     * @param ?Closure(ClientOptions): string $runIdGenerator Optional run id generator, called with the
     *     resolved per-call options when the request carries no {@see self::RUN_ID_HEADER}. Injected by
     *     the bundle from the `keboola_api.storage_client_options.run_id_generator` service id; owned
     *     here because 7.0's {@see ClientOptions} no longer carries a generator.
     */
    public function __construct(
        private readonly ClientOptions $baseClientOptions,
        private readonly ?Closure $runIdGenerator = null,
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

        if ($this->runIdGenerator !== null) {
            $runId = ($this->runIdGenerator)($options);
            assert(is_string($runId));
            return $runId;
        }

        return uniqid('run-');
    }
}
