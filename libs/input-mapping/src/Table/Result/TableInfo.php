<?php

namespace Keboola\InputMapping\Table\Result;

use Generator;

class TableInfo
{
    /** @var string */
    private $id;
    /** @var Column[] */
    private $columns;
    /** @var ?string */
    private $sourceTableId;
    /** @var string */
    private $lastImportDate;
    /** @var string */
    private $lastChangeDate;
    /** @var string */
    private $displayName;
    /** @var string */
    private $name;

    public function __construct(array $tableInfo)
    {
        $this->id = $tableInfo['id'];
        $this->displayName = $tableInfo['displayName'];
        $this->name = $tableInfo['name'];
        $this->lastImportDate = $tableInfo['lastImportDate'];
        $this->lastChangeDate = $tableInfo['lastChangeDate'];
        $this->sourceTableId = !empty($tableInfo['sourceTable']) ? $tableInfo['sourceTable']['id'] : null;
        foreach ($tableInfo['columns'] as $columnId) {
            $metadata = !empty($tableInfo['columnMetadata'][$columnId]) ? $tableInfo['columnMetadata'][$columnId] :
                (!empty($tableInfo['sourceTable']['columnMetadata'][$columnId]) ?
                    $tableInfo['sourceTable']['columnMetadata'][$columnId] : []);
            $this->columns[] = new Column($columnId, $metadata);
        }
    }

    /**
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @return string
     */
    public function getDisplayName()
    {
        return $this->displayName;
    }

    /**
     * @return string
     */
    public function getLastChangeDate()
    {
        return $this->lastChangeDate;
    }

    /**
     * @return string
     */
    public function getLastImportDate()
    {
        return $this->lastImportDate;
    }

    /**
     * @return string
     */
    public function getSourceTableId()
    {
        return $this->sourceTableId;
    }

    /**
     * @return Generator
     */
    public function getColumns()
    {
        foreach ($this->columns as $column) {
            yield $column;
        }
    }

    /**
     * @return string
     */
    public function getId()
    {
        return $this->id;
    }
}
