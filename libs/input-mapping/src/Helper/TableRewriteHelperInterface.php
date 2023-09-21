<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptionsList;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

interface TableRewriteHelperInterface
{
    public function rewriteTableOptionsSources(
        InputTableOptionsList $tablesDefinition,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
    ): RewrittenInputTableOptionsList;

    public function rewriteTableStatesDestinations(
        InputTableStateList $tableStates,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
    ): InputTableStateList;
}
