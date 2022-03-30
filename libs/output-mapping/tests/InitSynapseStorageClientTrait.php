<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\StorageApi\Exception;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;

trait InitSynapseStorageClientTrait
{
    protected function checkSynapseTests(): bool
    {
        if (!getenv('RUN_SYNAPSE_TESTS')) {
            return false;
        }
        if (getenv('SYNAPSE_STORAGE_API_TOKEN') === false) {
            throw new Exception('SYNAPSE_STORAGE_API_TOKEN must be set for synapse tests');
        }
        if (getenv('SYNAPSE_STORAGE_API_URL') === false) {
            throw new Exception('SYNAPSE_STORAGE_API_URL must be set for synapse tests');
        }
        return true;
    }

    protected function getSynapseClientWrapper(): ClientWrapper
    {
        $clientOptions = (new ClientOptions())
            ->setUrl((string) getenv('SYNAPSE_STORAGE_API_URL'),
                (string) getenv('SYNAPSE_STORAGE_API_TOKEN'),)
            ->setToken((string) getenv('SYNAPSE_STORAGE_API_TOKEN'))
            ->setBackoffMaxTries(1)
            ->setJobPollRetryDelay(function () {
                return 1;
            });
        $clientWrapper = new ClientWrapper($clientOptions);
        $tokenInfo = $clientWrapper->getBasicClient()->verifyToken();
        print(sprintf(
            'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
            $tokenInfo['description'],
            $tokenInfo['id'],
            $tokenInfo['owner']['name'],
            $tokenInfo['owner']['id'],
            $clientWrapper->getBasicClient()->getApiUrl()
        ));
        return $clientWrapper;
    }
}
