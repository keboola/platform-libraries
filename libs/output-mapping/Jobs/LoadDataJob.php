<?php

namespace Keboola\OutputMapping\Jobs;

use Keboola\StorageApi\Client;

class LoadDataJob extends BaseStorageJob
{
    /**
     * @var string
     */
    private $destination;

    /**
     * @var array
     */
    private $options;

    public function __construct(Client $client, $destination, array $options)
    {
        parent::__construct($client);
        $this->destination = $destination;
        $this->options = $options;
    }

    public function run()
    {
        return $this->client->queueTableImport($this->destination, $this->options);
    }

    public function isSynchronous()
    {
        return false;
    }
}
