<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Result;
use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\InputMapping\Table\StrategyInterface;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Psr\Log\LoggerInterface;

abstract class AbstractStrategy implements StrategyInterface
{
    protected readonly LoggerInterface $logger; // @phpstan-ignore-line initialized in child classes

    abstract protected function getMetadataStorage(): FileStagingInterface;
    abstract protected function getDestination(): string;


    /**
     * @param RewrittenInputTableOptions[] $tables
     * @param bool $preserve
     * @return Result
     */
    public function downloadTables(array $tables, bool $preserve): Result
    {
        $outputStateConfiguration = [];
        $exports = [];
        $result = new Result();
        foreach ($tables as $table) {
            $outputStateConfiguration[] = [
                'source' => $table->getSource(),
                'lastImportDate' => $table->getTableInfo()['lastImportDate'],
            ];
            $exports[] = $this->downloadTable($table);
            $this->logger->info('Fetched table ' . $table->getSource() . '.');
            $result->addTable(new TableInfo($table->getTableInfo()));
        }

        $result->setMetrics($this->handleExports($exports, $preserve));
        $result->setInputTableStateList(new InputTableStateList($outputStateConfiguration));
        $this->logger->info('All tables were fetched.');

        return $result;
    }
}
