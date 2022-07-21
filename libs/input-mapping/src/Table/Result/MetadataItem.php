<?php

namespace Keboola\InputMapping\Table\Result;

class MetadataItem
{
    /** @var string */
    private $key;
    /** @var string */
    private $value;
    /** @var string */
    private $provider;
    /** @var string */
    private $timestamp;

    /**
     * @param array $metadataItem
     */
    public function __construct(array $metadataItem)
    {
        $this->key = $metadataItem['key'];
        $this->value = $metadataItem['value'];
        $this->provider = $metadataItem['provider'];
        $this->timestamp = $metadataItem['timestamp'];
    }

    /**
     * @return string
     */
    public function getProvider()
    {
        return $this->provider;
    }

    /**
     * @return string
     */
    public function getValue()
    {
        return $this->value;
    }

    /**
     * @return string
     */
    public function getKey()
    {
        return $this->key;
    }

    /**
     * @return string
     */
    public function getTimestamp()
    {
        return $this->timestamp;
    }
}
