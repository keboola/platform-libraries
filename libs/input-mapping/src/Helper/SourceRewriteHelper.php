<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class SourceRewriteHelper
{
    public static function rewriteTableOptionsSources(
        InputTableOptionsList $tablesDefinition,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger
    ): InputTableOptionsList {
        if ($clientWrapper->hasBranch()) {
            foreach ($tablesDefinition->getTables() as $tableOptions) {
                $tableOptions->setSource(self::rewriteSource($tableOptions->getSource(), $clientWrapper, $logger));
            }
        }
        return $tablesDefinition;
    }

    public static function rewriteTableStatesDestinations(
        InputTableStateList $tableStates,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger
    ): InputTableStateList {
        if ($clientWrapper->hasBranch()) {
            $tableStates = $tableStates->jsonSerialize();
            foreach ($tableStates as &$tableState) {
                $tableState['source'] = self::rewriteSource($tableState['source'], $clientWrapper, $logger);
            }
            return new InputTableStateList($tableStates);
        }
        return $tableStates;
    }

    private static function getNewSource(string $source, string $branchName): string
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
        $bucketId = $branchName . '-' . $bucketId;
        $tableIdParts[1] = $prefix . $bucketId;
        return implode('.', $tableIdParts);
    }

    private static function rewriteSource(string $source, ClientWrapper $clientWrapper, LoggerInterface $logger): string
    {
        $newSource = self::getNewSource(
            $source,
            $clientWrapper->getBasicClient()->webalizeDisplayName((string) $clientWrapper->getBranchId())['displayName']
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
