<?php

namespace Keboola\OutputMapping\Jobs;

use function GuzzleHttp\Promise\all;
use GuzzleHttp\Promise\Promise;
use Keboola\StorageApi\Client;

class ParallelJob extends BaseStorageJob
{
    /**
     * @var array
     */
    private $jobs;

    /**
     * @var Promise[]
     */
    private $promises;

    public function __construct(Client $client, array $jobs)
    {
        parent::__construct($client);
        $this->jobs = $jobs;
    }

    public function run()
    {
        $this->promises = [];
        foreach ($this->jobs as $job) {
            assert($job instanceof SerialJob);
            $this->promises[] = $job->run();
        }
    }

    public function waitFor()
    {
        $results = [];
        foreach ($this->promises as $promise) {
            $promise->then(
                function ($value) use (&$results) {
                    $results[] = $value;
                    echo 'fullfilled';
                    var_dump($value);
                },
                function ($value) use (&$results) {
                    $results[] = $value;
                    echo 'rejected';
                    var_dump($value);
                }
            );
     //      $promise->resolve('value');
        }
        echo 'waiting to resolve';
        $val = all($this->promises)->wait();
        var_dump($val);
        /**
        foreach ($this->promises as $promise) {
            $value = $promise->wait();
            var_dump($value);
        }
         * */
   /*    while (count($results) < count($this->jobs)) {
            echo 'waiting';
            sleep(1);
        }
   */
        echo 'finished';
        return $val;

    }

    public function isSynchronous()
    {
        return false;
    }
}
