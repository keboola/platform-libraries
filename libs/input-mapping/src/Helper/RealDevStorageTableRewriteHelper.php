<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptionsList;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class RealDevStorageTableRewriteHelper implements TableRewriteHelperInterface
{
    public function rewriteTableOptionsSources(
        InputTableOptionsList $tablesDefinition,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger
    ): RewrittenInputTableOptionsList {
        $newTables = [];
        foreach ($tablesDefinition->getTables() as $tableOptions) {
            list($tableInfo, $sourceBranchId) = $this->rewriteSourceBranchId(
                $tableOptions->getSource(),
                $clientWrapper,
                $logger
            );
            $newTables[] = new RewrittenInputTableOptions(
                $tableOptions->getDefinition(),
                $tableOptions->getSource(),
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
        // Table names remain the same in this case
        return $tableStates;
    }

    private function rewriteSourceBranchId(
        string $source,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger
    ): array {
        if ($clientWrapper->isDevelopmentBranch() && $clientWrapper->getBranchClient()->tableExists($source)) {
            $logger->info(sprintf(
                'Using dev input "%s" from branch "%s" instead of main branch "%s".',
                $source,
                $clientWrapper->getBranchId(),
                $clientWrapper->getDefaultBranch()['branchId']
            ));
            $tableInfo = $clientWrapper->getBranchClient()->getTable($source);
            return [$tableInfo, $clientWrapper->getBranchId()];
        }
        $logger->info(sprintf(
            'Using fallback to default branch "%s" for input "%s".',
            $clientWrapper->getDefaultBranch()['branchId'],
            $source,
        ));
        // use production table
        $tableInfo = $clientWrapper->getBasicClient()->getTable($source);
        return [$tableInfo, $clientWrapper->getDefaultBranch()['branchId']];
    }
}
