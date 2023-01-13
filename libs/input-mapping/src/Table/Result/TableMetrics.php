<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Result;

class TableMetrics
{
    private int $compressedBytes;
    private int $uncompressedBytes;
    private string $tableId;

    public function __construct(array $jobResult)
    {
        $this->tableId = (string) $jobResult['tableId'];
        $this->compressedBytes = (int) $jobResult['metrics']['outBytes'];
        $this->uncompressedBytes = (int) $jobResult['metrics']['outBytesUncompressed'];
    }

    public function getUncompressedBytes(): int
    {
        return $this->uncompressedBytes;
    }

    public function getCompressedBytes(): int
    {
        return $this->compressedBytes;
    }

    public function getTableId(): string
    {
        return $this->tableId;
    }
}
