<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Result;

class MetadataItem
{
    private string $key;
    private string $value;
    private string $provider;
    private string $timestamp;

    /**
     * @param array $metadataItem
     */
    public function __construct(array $metadataItem)
    {
        $this->key = (string) $metadataItem['key'];
        $this->value = (string) $metadataItem['value'];
        $this->provider = (string) $metadataItem['provider'];
        $this->timestamp = (string) $metadataItem['timestamp'];
    }

    public function getProvider(): string
    {
        return $this->provider;
    }

    public function getValue(): string
    {
        return $this->value;
    }

    public function getKey(): string
    {
        return $this->key;
    }

    public function getTimestamp(): string
    {
        return $this->timestamp;
    }
}
