<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Result;

class TableInfo
{
    private string $id;
    /** @var Column[] */
    private array $columns;
    private ?string $sourceTableId;
    private ?string $lastImportDate;
    private ?string $lastChangeDate;
    private string $displayName;
    private string $name;

    public function __construct(array $tableInfo)
    {
        $this->id = (string) $tableInfo['id'];
        $this->displayName = (string) $tableInfo['displayName'];
        $this->name = (string) $tableInfo['name'];
        $this->lastImportDate = !empty($tableInfo['lastImportDate']) ? (string) $tableInfo['lastImportDate'] : null;
        $this->lastChangeDate = !empty($tableInfo['lastChangeDate']) ? (string) $tableInfo['lastChangeDate'] : null;
        $this->sourceTableId = !empty($tableInfo['sourceTable']) ? (string) $tableInfo['sourceTable']['id'] : null;
        foreach ($tableInfo['columns'] as $columnId) {
            $metadata = !empty($tableInfo['columnMetadata'][$columnId]) ? $tableInfo['columnMetadata'][$columnId] :
                (!empty($tableInfo['sourceTable']['columnMetadata'][$columnId]) ?
                    $tableInfo['sourceTable']['columnMetadata'][$columnId] : []);
            $this->columns[] = new Column($columnId, $metadata);
        }
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getDisplayName(): ?string
    {
        return $this->displayName;
    }

    public function getLastChangeDate(): ?string
    {
        return $this->lastChangeDate;
    }

    public function getLastImportDate(): ?string
    {
        return $this->lastImportDate;
    }

    public function getSourceTableId(): ?string
    {
        return $this->sourceTableId;
    }

    public function getColumns(): array
    {
        return $this->columns;
    }

    public function getId(): string
    {
        return $this->id;
    }
}
