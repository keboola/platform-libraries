<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Configuration\Table;

use Generator;
use Keboola\OutputMapping\Configuration\Table\Webalizer;
use Keboola\StorageApi\Client;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Monolog\Handler\TestHandler;
use Monolog\Level;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use PHPUnit\Util\Test;

class WebalizerTest extends TestCase
{
    private ClientWrapper $clientWrapper;

    public function setUp(): void
    {
        parent::setUp();

        $this->initClient();
    }

    /** @dataProvider webalizeDataProvider */
    public function testWebalize(array $config, array $expectedConfig, int $expectsApiCalls): void
    {
        $logger = new Logger('test');

        // test webalized column names
        $webalizator = new Webalizer($this->clientWrapper->getBranchClient(), $logger, true);
        $webalizedColumnNames = $webalizator->webalize($config);
        self::assertEquals($expectedConfig, $webalizedColumnNames);

        // test the number of API calls
        $clientMock = self::createMock(Client::class);
        $clientMock
            ->expects(self::exactly($expectsApiCalls))
            ->method('webalizeColumnNames')->will(
                self::returnCallback(
                    function ($columns) {
                        return ['columnNames' => $columns];
                    },
                ),
            );

        $webalizator = new Webalizer($clientMock, $logger, true);
        self::assertIsArray($webalizator->webalize($config));
    }

    /** @dataProvider webalizeDataProvider */
    public function testLegacyWebalize(array $config, array $expectedConfig): void
    {
        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::never())->method('webalizeColumnNames');

        $webalizator = new Webalizer($clientMock, new Logger('test'), false);
        self::assertEquals($expectedConfig, $webalizator->webalize($config));
    }

    public function testLoggingWebalizedColumnNames(): void
    {
        $logsHandler = new TestHandler();
        $logger = new Logger('test', [$logsHandler]);

        $webalizator = new Webalizer($this->clientWrapper->getBranchClient(), $logger, true);
        $webalizator->webalize([
            'columns' => ['ěščřžýáíéúů'],
            'primary_key' => ['éíěčíáčšžášřýšěí'],
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
        self::assertCount(4, $logsHandler->getRecords());
        self::assertSame(Level::Warning, $logsHandler->getRecords()[0]->level);
        self::assertSame(Level::Warning, $logsHandler->getRecords()[1]->level);
        self::assertSame(Level::Warning, $logsHandler->getRecords()[2]->level);
        self::assertSame(Level::Warning, $logsHandler->getRecords()[3]->level);
        self::assertSame(
            'Column name "ěščřžýáíéúů" was webalized to "escrzyaieuu"',
            $logsHandler->getRecords()[0]->message,
        );
        self::assertSame(
            'Column name "éíěčíáčšžášřýšěí" was webalized to "eieciacszasrysei"',
            $logsHandler->getRecords()[1]->message,
        );
        self::assertSame(
            'Column name "webalize | test 😁" was webalized to "webalize_test"',
            $logsHandler->getRecords()[2]->message,
        );
        self::assertSame(
            'Column name "    webalize spaces  " was webalized to "webalize_spaces"',
            $logsHandler->getRecords()[3]->message,
        );
    }

    public function webalizeDataProvider(): Generator
    {
        yield 'empty' => [
            [],
            [],
            0,
        ];

        yield 'simple' => [
            [
                'columns' => ['col1', 'col2'],
                'primary_key' => ['col1'],
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
                'primary_key' => ['col1'],
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
            4,
        ];

        yield 'special-chars' => [
            [
                'columns' => ['ěščřžýáíéúů', 'webalize | test 😁'],
                'primary_key' => ['ěščřžýáíéúů'],
                'column_metadata' => [
                    'webalize | test 😁' => [
                        'key' => 'key1',
                        'val' => 'val1',
                    ],
                    '1' => [
                        'key' => 'key2',
                        'val' => 'val2',
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
                'primary_key' => ['escrzyaieuu'],
                'column_metadata' => [
                    'webalize_test' => [
                        'key' => 'key1',
                        'val' => 'val1',
                    ],
                    '1' => [
                        'key' => 'key2',
                        'val' => 'val2',
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
            4,
        ];

        yield 'system-columns' => [
            [
                'columns' => ['_timestamp'],
                'primary_key' => ['_timestamp'],
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
                'primary_key' => ['_timestamp'],
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
            4,
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
