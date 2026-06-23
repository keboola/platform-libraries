<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\StorageApiClient;

use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\AuthType;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientFactoryInterface;
use Keboola\StorageApiBranch\StorageApiToken;
use Symfony\Component\HttpFoundation\Request;

class StorageClientApiFactory implements StorageClientFactoryInterface
{
    public const RUN_ID_HEADER = 'X-KBC-RunId';

    private ClientOptions $clientOptions;

    public function __construct(ClientOptions $clientOptions)
    {
        $this->clientOptions = new ClientOptions();
        $this->clientOptions->addValuesFrom($clientOptions);
    }

    public function getClientOptionsReadOnly(): ClientOptions
    {
        return clone $this->clientOptions;
    }

    public function createClientWrapper(
        Request $request,
        StorageApiToken $storageToken,
        ?ClientOptions $clientOptions = null,
    ): ClientWrapper {
        $options = clone $this->clientOptions;
        if ($clientOptions) {
            $options->addValuesFrom($clientOptions);
        }

        $options->setToken($storageToken->getTokenValue());
        $options->setAuthType(AuthType::STORAGE_TOKEN);
        $options->setRunId($this->getRunId($request, $options));

        return new ClientWrapper($options);
    }

    private function getRunId(Request $request, ClientOptions $options): string
    {
        $runId = (string) $request->headers->get(self::RUN_ID_HEADER);

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
