<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptions;
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
        LoggerInterface $logger,
    ): RewrittenInputTableOptionsList {
        $newTables = [];
        foreach ($tablesDefinition->getTables() as $tableOptions) {
            list($tableInfo, $sourceBranchId) = $this->rewriteSourceBranchId(
                $tableOptions,
                $clientWrapper,
                $logger,
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
        LoggerInterface $logger,
    ): InputTableStateList {
        // Table names remain the same in this case
        return $tableStates;
    }

    private function rewriteSourceBranchId(
        InputTableOptions $tableOptions,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
    ): array {
        $source = $tableOptions->getSource();
        $defaultBranchId = $clientWrapper->getDefaultBranch()->id;

        if ($tableOptions->getSourceBranchId() !== null) {
            // use source branch from mapping definition
            $branchId = (string) $tableOptions->getSourceBranchId();
            $logger->info(sprintf(
                'Using input "%s" from %s branch "%s".',
                $source,
                $branchId === $defaultBranchId ? 'main' : 'dev',
                $branchId,
            ));

            return [
                $clientWrapper->getClientForBranch($branchId)
                    ->getTable($tableOptions->getSource())
                ,
                $branchId,
            ];
        }

        if ($clientWrapper->isDevelopmentBranch() && $clientWrapper->getBranchClient()->tableExists($source)) {
            $logger->info(sprintf(
                'Using dev input "%s" from branch "%s" instead of main branch "%s".',
                $source,
                $clientWrapper->getBranchId(),
                $defaultBranchId,
            ));
            $tableInfo = $clientWrapper->getBranchClient()->getTable($source);
            return [$tableInfo, $clientWrapper->getBranchId()];
        }

        $logger->info(sprintf(
            'Using fallback to default branch "%s" for input "%s".',
            $defaultBranchId,
            $source,
        ));
        // use production table
        $tableInfo = $clientWrapper->getClientForDefaultBranch()->getTable($source);
        return [$tableInfo, $defaultBranchId];
    }
}
