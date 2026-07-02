<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\StorageApiClient;

use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Symfony\Component\HttpFoundation\Request;

class StorageClientApiFactory
{
    public const RUN_ID_HEADER = 'X-KBC-RunId';

    public function __construct(
        private readonly ClientOptions $baseClientOptions,
        private readonly Request $request,
        private readonly StorageApiToken $token,
    ) {
    }

    public function createClientWrapper(?ClientOptions $clientOptions = null): ClientWrapper
    {
        $options = clone $this->baseClientOptions;
        if ($clientOptions !== null) {
            $options->addValuesFrom($clientOptions);
        }

        $options->setToken($this->token->getTokenValue());
        $options->setAuthType($this->token->getTokenType());
        $options->setRunId($this->getRunId($options));

        return new ClientWrapper($options);
    }

    private function getRunId(ClientOptions $options): string
    {
        $runId = (string) $this->request->headers->get(self::RUN_ID_HEADER);

        if ($runId === '') {
            $runIdGenerator = $options->getRunIdGenerator();
            if ($runIdGenerator !== null) {
                $runId = $runIdGenerator($options);
                assert(is_string($runId));
            } else {
                $runId = uniqid('run-');
            }
        }

        return $runId;
    }
}
