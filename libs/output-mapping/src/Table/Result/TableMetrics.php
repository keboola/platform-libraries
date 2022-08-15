<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Table\Result;

class TableMetrics
{
    private int $compressedBytes;
    private int $uncompressedBytes;
    private string $tableId;

    public function __construct(array $jobResult)
    {
        $this->tableId = (string) $jobResult['tableId'];
        $this->compressedBytes = (int) $jobResult['metrics']['inBytes'];
        $this->uncompressedBytes = (int) $jobResult['metrics']['inBytesUncompressed'];
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
