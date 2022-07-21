<?php

namespace Keboola\InputMapping\Table\Result;

class TableMetrics
{
    /** @var int */
    private $compressedBytes;

    /** @var int */
    private $uncompressedBytes;

    /** @var string */
    private $tableId;

    /**
     * @param array $jobResult
     */
    public function __construct(array $jobResult)
    {
        $this->tableId = $jobResult['tableId'];
        $this->compressedBytes = $jobResult['metrics']['outBytes'];
        $this->uncompressedBytes = $jobResult['metrics']['outBytesUncompressed'];
    }

    /**
     * @return int
     */
    public function getUncompressedBytes()
    {
        return $this->uncompressedBytes;
    }

    /**
     * @return int
     */
    public function getCompressedBytes()
    {
        return $this->compressedBytes;
    }

    /**
     * @return string
     */
    public function getTableId()
    {
        return $this->tableId;
    }
}
