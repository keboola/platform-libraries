<?php

namespace Keboola\OutputMapping\Jobs;

use GuzzleHttp\Promise\Promise;
use Keboola\StorageApi\Client;

class SerialJob extends BaseStorageJob
{
    /**
     * @var JobInterface[]
     */
    private $children;

    /**
     * @var Promise[]
     */
    private $promises;

    public function __construct(Client $client, array $children)
    {
        parent::__construct($client);
        $this->children = $children;
    }

    public function run()
    {
        $this->promises = [];
        for ($i = 0; $i < count($this->children); $i++) {
            $this->promises[$i] = new Promise(
                function () use ($i) {
                    if ($this->children[$i]->isSynchronous()) {
                        $result = $this->children[$i]->run();
                    } else {
                        $jobId = $this->children[$i]->run();
                        $result = $this->client->waitForJob($jobId);
                    }
                    $this->promises[$i]->resolve($result);
                }
            );
           // $this->promises[] = $promise;
        }
        for ($i = 0; $i < count($this->promises) - 1; $i++) {
            //$nextPromise = $this->promises[$i + 1];
            $this->promises[$i]->then(function ($value) use ($i) {
                var_dump($value);
                return $this->promises[$i + 1];
            });
        }
        return $this->promises[0];
    }

    public function isSynchronous()
    {
        return true;
    }
}
