<?php

namespace Keboola\OutputMapping\Jobs;

use Keboola\StorageApi\Client;

class LoadTableJob extends BaseStorageJob
{
    /**
     * @var string
     */
    private $destination;

    /**
     * @var array
     */
    private $options;

    /**
     * @var JobInterface[]
     */
    private $children;

    /**
     * @var string
     */
    private $jobId;

    public function __construct(Client $client, $destination, array $options)
    {
        parent::__construct($client);
        $this->destination = $destination;
        $this->options = $options;
    }

    public function run()
    {
        $this->jobId = $this->client->queueTableImport($this->destination, $this->options);
        return $this->jobId;
    }

    public function getId()
    {
        return $this->jobId;
    }

    public function waitForChildren()
    {
        $results = [];
        foreach ($this->children as $child) {
            $results[] = $child->run();
        }
        return $results;
    }

    public function addChild(BaseStorageJob $job)
    {
        $this->children[] = $job;
    }
}
