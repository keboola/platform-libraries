<?php

namespace Keboola\OutputMapping\Tests\Writer;

class BaseWriterMetadataTest extends BaseWriterTest
{
    /**
     * Transform metadata into a key-value array
     * @param $metadata
     * @return array
     */
    protected function getMetadataValues($metadata)
    {
        $result = [];
        foreach ($metadata as $item) {
            $result[$item['provider']][$item['key']] = $item['value'];
        }
        return $result;
    }
}
