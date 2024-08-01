<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Configuration\Table;

use Generator;
use Keboola\OutputMapping\Configuration\Table\Webalizer;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use PHPUnit\Util\Test;
use Psr\Log\Test\TestLogger;

class WebalizerTest extends TestCase
{
    private ClientWrapper $clientWrapper;

    public function setUp(): void
    {
        parent::setUp();

        $this->initClient();
    }

    /** @dataProvider webalizeDataProvider */
    public function testWebalize(array $config, array $expectedConfig): void
    {
        $webalizator = new Webalizer($this->clientWrapper->getBranchClient(), new TestLogger());
        self::assertEquals($expectedConfig, $webalizator->webalize($config));
    }

    public function testLoggingWebalizedColumnNames(): void
    {
        $testLogger = new TestLogger();
        $webalizator = new Webalizer($this->clientWrapper->getBranchClient(), $testLogger);
        $webalizator->webalize([
            'columns' => ['ěščřžýáíéúů'],
            'primaryKey' => ['éíěčíáčšžášřýšěí'],
            'column_metadata' => [
                'webalize | test 😁' => [
                    'key' => 'key1',
                    'val' => 'val1',
                ],
            ],
            'schema' => [
                [
                    'name' => '    webalize spaces  ',
                ],
                [
                    'name' => 'col3',
                ],
            ],
        ]);
        self::assertCount(4, $testLogger->records);
        self::assertEquals(
            'Column name "ěščřžýáíéúů" was webalized to "escrzyaieuu"',
            $testLogger->records[0]['message'],
        );
        self::assertEquals(
            'Column name "éíěčíáčšžášřýšěí" was webalized to "eieciacszasrysei"',
            $testLogger->records[1]['message'],
        );
        self::assertEquals(
            'Column name "webalize | test 😁" was webalized to "webalize_test"',
            $testLogger->records[2]['message'],
        );
        self::assertEquals(
            'Column name "    webalize spaces  " was webalized to "webalize_spaces"',
            $testLogger->records[3]['message'],
        );
    }

    public function webalizeDataProvider(): Generator
    {
        yield 'empty' => [
            [],
            [],
        ];

        yield 'simple' => [
            [
                'columns' => ['col1', 'col2'],
                'primaryKey' => ['col1'],
                'column_metadata' => [
                    'col1' => [
                        'name' => 'col1',
                    ],
                ],
                'schema' => [
                    [
                        'name' => 'col1',
                    ],
                ],
            ],
            [
                'columns' => ['col1', 'col2'],
                'primaryKey' => ['col1'],
                'column_metadata' => [
                    'col1' => [
                        'name' => 'col1',
                    ],
                ],
                'schema' => [
                    [
                        'name' => 'col1',
                    ],
                ],
            ],
        ];

        yield 'special-chars' => [
            [
                'columns' => ['ěščřžýáíéúů', 'webalize | test 😁'],
                'primaryKey' => ['ěščřžýáíéúů'],
                'column_metadata' => [
                    'webalize | test 😁' => [
                        'key' => 'key1',
                        'val' => 'val1',
                    ],
                ],
                'schema' => [
                    [
                        'name' => 'webalize | test 😁',
                    ],
                    [
                        'name' => 'ěščřžýáíéúů',
                    ],
                    [
                        'name' => 'col3',
                    ],
                ],
            ],
            [
                'columns' => ['escrzyaieuu', 'webalize_test'],
                'primaryKey' => ['escrzyaieuu'],
                'column_metadata' => [
                    'webalize_test' => [
                        'key' => 'key1',
                        'val' => 'val1',
                    ],
                ],
                'schema' => [
                    [
                        'name' => 'webalize_test',
                    ],
                    [
                        'name' => 'escrzyaieuu',
                    ],
                    [
                        'name' => 'col3',
                    ],
                ],
            ],
        ];

        yield 'system-columns' => [
            [
                'columns' => ['_timestamp'],
                'primaryKey' => ['_timestamp'],
                'column_metadata' => [
                    '_timestamp' => [
                        'key' => 'key1',
                        'val' => 'val1',
                    ],
                ],
                'schema' => [
                    [
                        'name' => '_timestamp',
                    ],
                ],
            ],
            [
                'columns' => ['_timestamp'],
                'primaryKey' => ['_timestamp'],
                'column_metadata' => [
                    '_timestamp' => [
                        'key' => 'key1',
                        'val' => 'val1',
                    ],
                ],
                'schema' => [
                    [
                        'name' => '_timestamp',
                    ],
                ],
            ],
        ];
    }

    protected function initClient(?string $branchId = null): void
    {
        $clientOptions = (new ClientOptions())
            ->setUrl((string) getenv('STORAGE_API_URL'))
            ->setToken((string) getenv('STORAGE_API_TOKEN'))
            ->setBranchId($branchId)
            ->setBackoffMaxTries(1)
            ->setJobPollRetryDelay(function () {
                return 1;
            })
            ->setUserAgent(implode('::', Test::describe($this)));
        $this->clientWrapper = new ClientWrapper($clientOptions);
    }
}
