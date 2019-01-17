<?php

namespace Keboola\OutputMapping\Jobs;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\StorageApi\Client;

class JobRunner
{
    /**
     * @var LoadTableJob[]
     */
    private $loadTableJobs;

    /**
     * @var Client
     */
    private $client;

    public function __construct(Client $client, array $loadTableJobs)
    {
        $this->client = $client;
        $this->loadTableJobs = $loadTableJobs;
    }

    public function start()
    {
        foreach ($this->loadTableJobs as $job) {
            $job->run();
        }
    }

    public function waitForAll()
    {
        $allResults = [];
        foreach ($this->loadTableJobs as $job) {
            $jobResult = $this->client->waitForJob($job->getId());
            if ($jobResult['status'] == 'error') {
                throw new InvalidOutputException($jobResult['error']['message'], null);
            }
            $job->waitForChildren();
            $allResults[] = $job->getId();
        }
        return $allResults;
    }
}
