<?php

namespace Keboola\OutputMapping\Jobs;

use Keboola\StorageApi\Client;

abstract class BaseStorageJob
{
    /**
     * @var Client
     */
    protected $client;

    public function __construct(Client $client)
    {
        $this->client = $client;
    }
}
