<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests;

use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;

trait InitSynapseStorageClientTrait
{
    protected function getSynapseClientWrapper(): ClientWrapper
    {
        $clientOptions = (new ClientOptions())
            ->setUrl((string) getenv('SYNAPSE_STORAGE_API_URL'))
            ->setToken((string) getenv('SYNAPSE_STORAGE_API_TOKEN'))
            ->setBackoffMaxTries(1)
            ->setJobPollRetryDelay(function () {
                return 1;
            });
        $clientWrapper = new ClientWrapper($clientOptions);
        $tokenInfo = $clientWrapper->getBranchClient()->verifyToken();
        print(sprintf(
            'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
            $tokenInfo['description'],
            $tokenInfo['id'],
            $tokenInfo['owner']['name'],
            $tokenInfo['owner']['id'],
            $clientWrapper->getBranchClient()->getApiUrl(),
        ));
        return $clientWrapper;
    }
}
