<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class FakeDevStorageTableRewriteHelper implements TableRewriteHelperInterface
{
    public function rewriteTableOptionsSources(
        InputTableOptionsList $tablesDefinition,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger
    ): InputTableOptionsList {
        foreach ($tablesDefinition->getTables() as $tableOptions) {
            if ($clientWrapper->hasBranch()) {
                $tableOptions->setSource($this->rewriteSource($tableOptions->getSource(), $clientWrapper, $logger));
            }
            $tableOptions->setSourceBranchId($clientWrapper->getDefaultBranch()['branchId']);
        }
        return $tablesDefinition;
    }

    public function rewriteTableStatesDestinations(
        InputTableStateList $tableStates,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger
    ): InputTableStateList {
        if ($clientWrapper->hasBranch()) {
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
            $clientWrapper->getBranchClientIfAvailable()->webalizeDisplayName(
                (string) $clientWrapper->getBranchId()
            )['displayName']
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
