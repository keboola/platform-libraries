<?php

namespace Keboola\InputMapping\Reader\Helper;

use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class SourceRewriteHelper
{
    public static function rewriteTableOptionsDestinations(
        InputTableOptionsList $tablesDefinition,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger
    ) {
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
    ) {
        if ($clientWrapper->hasBranch()) {
            $tableStates = $tableStates->jsonSerialize();
            foreach ($tableStates as &$tableState) {
                $tableState['source'] = self::rewriteSource($tableState['source'], $clientWrapper, $logger);
            }
            return new InputTableStateList($tableStates);
        }
        return $tableStates;
    }

    private static function getNewSource($source, $branchName)
    {
        $tableIdParts = explode('.', $source);
        if (count($tableIdParts) !== 3) {
            throw new InputOperationException(sprintf('Invalid destination: "%s"', $source));
        }
        $bucketId = $tableIdParts[1];
        if (substr($bucketId, 0, 2) === 'c-') {
            $bucketId = substr($bucketId, 2);
        }
        $bucketId = $branchName . '-' . $bucketId;
        // this assumes that bucket id starts with c-
        // https://github.com/keboola/output-mapping/blob/f6451d2faa825913db2ce986952a9ad6db082e50/src/Writer/TableWriter.php#L498
        $tableIdParts[1] = 'c-' . $bucketId;
        return implode('.', $tableIdParts);
    }

    private static function rewriteSource($source, ClientWrapper $clientWrapper, LoggerInterface $logger)
    {
        $newSource = self::getNewSource(
            $source,
            $clientWrapper->getBasicClient()->webalizeDisplayName((string) $clientWrapper->getBranchId())['displayName']
        );
        if ($clientWrapper->getBasicClient()->tableExists($newSource)) {
            $logger->info(
                sprintf('Using dev input "%s" instead of "%s".', $newSource, $source)
            );
            return $newSource;
        } else {
            return $source;
        }
    }
}
