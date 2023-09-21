<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Configuration\Adapter;
use Keboola\InputMapping\Helper\ManifestCreator;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptionsList;
use Keboola\InputMapping\Table\Result;
use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\InputMapping\Table\StrategyInterface;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

abstract class AbstractStrategy implements StrategyInterface
{
    protected ManifestCreator $manifestCreator;

    /**
     * @param Adapter::FORMAT_YAML | Adapter::FORMAT_JSON $format
     */
    public function __construct(
        protected readonly ClientWrapper $clientWrapper,
        protected readonly LoggerInterface $logger,
        protected readonly ProviderInterface $dataStorage,
        protected readonly ProviderInterface $metadataStorage,
        protected readonly InputTableStateList $tablesState,
        protected readonly string $destination,
        protected readonly string $format = 'json',
    ) {
        $this->manifestCreator = new ManifestCreator();
    }

    protected function ensurePathDelimiter(string $path): string
    {
        return $this->ensureNoPathDelimiter($path) . '/';
    }

    protected function ensureNoPathDelimiter(string $path): string
    {
        return rtrim($path, '\\/');
    }

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

    protected function getDestinationFilePath(string $destination, InputTableOptions $table): string
    {
        if (!$table->getDestination()) {
            return $destination . '/' . $table->getSource();
        } else {
            return $destination . '/' . $table->getDestination();
        }
    }
}
