<?php

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\InputMapping\Table\Strategy\Exasol;
use Keboola\StorageApi\Client;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class ExasolTest extends TestCase
{
    public function testExasolDownloadTable(): void
    {
        $clientWrapper = new ClientWrapper(
            new Client(['token' => STORAGE_API_TOKEN, "url" => STORAGE_API_URL]),
            null,
            null
        );
        $strategy = new Exasol(
            $clientWrapper,
            new NullLogger(),
            new NullProvider(),
            new NullProvider(),
            new InputTableStateList([]),
            'test'
        );
        $result = $strategy->downloadTable(new InputTableOptions(
            ['source' => 'in.c-main.test', 'destination' => 'my-table', 'columns' => ['foo', 'bar']]
        ));
        self::assertEquals(
            [
                'table' => [
                    new InputTableOptions(
                        ['source' => 'in.c-main.test', 'destination' => 'my-table', 'columns' => ['foo', 'bar']]
                    ),
                    [
                        'columns' => ['foo', 'bar'],
                        'overwrite' => false,
                    ],
                ],
                'type' => 'copy',
            ],
            $result
        );
    }
}
