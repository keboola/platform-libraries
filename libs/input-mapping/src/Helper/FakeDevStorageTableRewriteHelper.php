<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptionsList;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class FakeDevStorageTableRewriteHelper implements TableRewriteHelperInterface
{
    public function rewriteTableOptionsSources(
        InputTableOptionsList $tablesDefinition,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger
    ): RewrittenInputTableOptionsList {
        $newTables = [];
        foreach ($tablesDefinition->getTables() as $tableOptions) {
            $source = $tableOptions->getSource();
            if ($clientWrapper->isDevelopmentBranch()) {
                $source = $this->rewriteSource($tableOptions->getSource(), $clientWrapper, $logger);
            }
            $sourceBranchId = $clientWrapper->getDefaultBranch()->id;
            $tableInfo = $clientWrapper->getTableAndFileStorageClient()->getTable($source);
            $newTables[] = new RewrittenInputTableOptions(
                $tableOptions->getDefinition(),
                $source,
                (int) $sourceBranchId,
                $tableInfo,
            );
        }
        return new RewrittenInputTableOptionsList($newTables);
    }

    public function rewriteTableStatesDestinations(
        InputTableStateList $tableStates,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger
    ): InputTableStateList {
        if ($clientWrapper->isDevelopmentBranch()) {
            $tableStates = $tableStates->jsonSerialize();
            foreach ($tableStates as &$tableState) {
                $tableState['source'] = $this->rewriteSource($tableState['source'], $clientWrapper, $logger);
            }
            return new InputTableStateList($tableStates);
        }
        return $tableStates;
    }

    private function getNewSource(string $source, string $branchId): string
    {
        $tableIdParts = explode('.', $source);
        if (count($tableIdParts) !== 3) {
            throw new InputOperationException(sprintf('Invalid destination: "%s"', $source));
        }
        $bucketId = $tableIdParts[1];
        $prefix = '';
        if (str_starts_with($bucketId, 'c-')) {
            $bucketId = substr($bucketId, 2);
            $prefix = 'c-';
        }
        $bucketId = $branchId . '-' . $bucketId;
        $tableIdParts[1] = $prefix . $bucketId;
        return implode('.', $tableIdParts);
    }

    private function rewriteSource(string $source, ClientWrapper $clientWrapper, LoggerInterface $logger): string
    {
        $newSource = $this->getNewSource(
            $source,
            (string) $clientWrapper->getBranchId()
        );
        if ($clientWrapper->getTableAndFileStorageClient()->tableExists($newSource)) {
            $logger->info(
                sprintf('Using dev input "%s" instead of "%s".', $newSource, $source)
            );
            return $newSource;
        } else {
            return $source;
        }
    }
}
