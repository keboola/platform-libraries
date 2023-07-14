<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\Exasol;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Psr\Log\NullLogger;

class ExasolTest extends AbstractTestCase
{
    #[NeedsTestTables]
    public function testExasolDownloadTable(): void
    {
        $strategy = new Exasol(
            $this->clientWrapper,
            new NullLogger(),
            new NullProvider(),
            new NullProvider(),
            new InputTableStateList([]),
            'test'
        );
        $result = $strategy->downloadTable(new RewrittenInputTableOptions(
            [
                'source' => $this->firstTableId,
                'destination' => 'my-table',
                'columns' => ['foo', 'bar'],
            ],
            $this->firstTableId,
            (int) $this->clientWrapper->getDefaultBranch()['branchId'],
            $this->clientWrapper->getBasicClient()->getTable($this->firstTableId),
        ));
        self::assertEquals(
            [
                'table' => [
                    new RewrittenInputTableOptions(
                        [
                            'source' => $this->firstTableId,
                            'destination' => 'my-table',
                            'columns' => ['foo', 'bar'],
                        ],
                        $this->firstTableId,
                        (int) $this->clientWrapper->getDefaultBranch()['branchId'],
                        $this->clientWrapper->getBasicClient()->getTable($this->firstTableId),
                    ),
                    [
                        'columns' => [
                            ['source' => 'foo'],
                            ['source' => 'bar'],
                        ],
                        'overwrite' => false,
                    ],
                ],
                'type' => 'copy',
            ],
            $result
        );
    }
}
