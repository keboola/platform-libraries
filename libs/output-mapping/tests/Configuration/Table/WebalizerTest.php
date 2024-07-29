<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Configuration\Table;

use Generator;
use Keboola\OutputMapping\Configuration\Table\Webalizer;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
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
    public function testWebalize(array $config, array $expectedConfig): void
    {
        $webalizator = new Webalizer($this->clientWrapper->getBranchClient());
        self::assertEquals($expectedConfig, $webalizator->webalize($config));
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
