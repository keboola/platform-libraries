<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use InvalidArgumentException;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\Writer\Helper\RestrictedColumnsHelper;
use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategy;

class TableConfigurationValidator
{
    public function validate(
        StrategyInterface                                      $strategy,
        MappingFromRawConfigurationAndPhysicalDataWithManifest $source,
        array                                                  $config,
    ): void {
        if (!$strategy instanceof SqlWorkspaceTableStrategy) {
            try {
                RestrictedColumnsHelper::validateRestrictedColumnsInConfig(
                    $config['columns'],
                    $config['column_metadata'],
                );
            } catch (InvalidOutputException $e) {
                throw new InvalidOutputException(
                    sprintf(
                        'Failed to process mapping for table %s: %s',
                        $source->getSourceName(),
                        $e->getMessage(),
                    ),
                    0,
                    $e,
                );
            }
        }

        if (!$config['columns'] && $source->isSliced()) {
            throw new InvalidOutputException(
                sprintf('Sliced file "%s" columns specification missing.', $source->getSourceName()),
            );
        }

        try {
            new MappingDestination($config['destination']);
        } catch (InvalidArgumentException $e) {
            throw new InvalidOutputException(sprintf(
                'Failed to resolve valid destination. "%s" is not a valid table ID.',
                $config['destination'],
            ), 0, $e);
        }
    }
}
