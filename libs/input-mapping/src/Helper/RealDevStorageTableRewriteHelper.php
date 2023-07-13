<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class RealDevStorageTableRewriteHelper implements TableRewriteHelperInterface
{
    public function rewriteTableOptionsSources(
        InputTableOptionsList $tablesDefinition,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger
    ): InputTableOptionsList {
        foreach ($tablesDefinition->getTables() as $tableOptions) {
            $tableOptions->setSourceBranchId(
                $this->rewriteSourceBranchId($tableOptions->getSource(), $clientWrapper, $logger)
            );
        }
        return $tablesDefinition;
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
    ): string {
        if ($clientWrapper->hasBranch() && $clientWrapper->getBranchClient()->tableExists($source)) {
            $logger->info(sprintf(
                'Using dev input "%s" from branch "%s" instead of main branch "%s".',
                $source,
                $clientWrapper->getBranchId(),
                $clientWrapper->getDefaultBranch()['branchId']
            ));
            return (string) $clientWrapper->getBranchId();
        }
        $logger->info(sprintf(
            'Using fallback to default branch "%s" for input "%s".',
            $clientWrapper->getDefaultBranch()['branchId'],
            $source,
        ));
        // use production table
        return $clientWrapper->getDefaultBranch()['branchId'];
    }
}
