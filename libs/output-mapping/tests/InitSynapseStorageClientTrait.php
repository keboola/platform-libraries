<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\Exception;
use Keboola\StorageApiBranch\ClientWrapper;

trait InitSynapseStorageClientTrait
{
    /**
     * @return bool
     * @throws Exception
     */
    protected function checkSynapseTests()
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

    /**
     * @return ClientWrapper
     */
    protected function getSynapseClientWrapper()
    {
        $clientWrapper = new ClientWrapper(
            new Client([
                'url' => (string) getenv('SYNAPSE_STORAGE_API_URL'),
                'token' => (string) getenv('SYNAPSE_STORAGE_API_TOKEN'),
                'backoffMaxTries' => 1,
                'jobPollRetryDelay' => function () {
                    return 1;
                },
            ]),
            null,
            null
        );
        $clientWrapper->setBranchId('');
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
