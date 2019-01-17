<?php

namespace Keboola\OutputMapping\Jobs;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\Metadata;

class PostTableMetadataJob extends BaseStorageJob
{
    /**
     * @var string
     */
    private $destination;

    /**
     * @var string
     */
    private $provider;

    /**
     * @var array
     */
    private $metadata;

    public function __construct(Client $client, $destination, $provider, $metadata)
    {
        parent::__construct($client);
        $this->destination = $destination;
        $this->provider = $provider;
        $this->metadata = $metadata;
    }

    public function run()
    {
        $metadataClient = new Metadata($this->client);
        return $metadataClient->postTableMetadata($this->destination, $this->provider, $this->metadata);
    }
}
