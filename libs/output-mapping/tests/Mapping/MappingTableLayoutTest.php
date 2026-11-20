<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Generator;
use Keboola\OutputMapping\Mapping\MappingTableLayout;
use Keboola\OutputMapping\Storage\TableInfo;
use PHPUnit\Framework\TestCase;

class MappingTableLayoutTest extends TestCase
{
    private const TIME_PARTITIONING = ['type' => 'DAY', 'field' => 'report_date'];
    private const RANGE_PARTITIONING = [
        'field' => 'id',
        'range' => ['start' => 0, 'end' => 100, 'interval' => 10],
    ];
    private const CLUSTERING = ['fields' => ['country', 'city']];

    public function testIsEmpty(): void
    {
        self::assertTrue((new MappingTableLayout(null, null, null))->isEmpty());
        self::assertFalse((new MappingTableLayout(self::TIME_PARTITIONING, null, null))->isEmpty());
        self::assertFalse((new MappingTableLayout(null, self::RANGE_PARTITIONING, null))->isEmpty());
        self::assertFalse((new MappingTableLayout(null, null, self::CLUSTERING))->isEmpty());
    }

    /**
     * @dataProvider requestDataProvider
     */
    public function testToRequestData(
        ?array $timePartitioning,
        ?array $rangePartitioning,
        ?array $clustering,
        array $expected,
    ): void {
        $layout = new MappingTableLayout($timePartitioning, $rangePartitioning, $clustering);

        self::assertSame($expected, $layout->toRequestData());
        self::assertSame(json_encode($expected), $layout->describeRequested());
    }

    public function requestDataProvider(): Generator
    {
        yield 'nothing' => [null, null, null, []];
        yield 'time partitioning' => [
            self::TIME_PARTITIONING,
            null,
            null,
            ['timePartitioning' => ['type' => 'DAY', 'field' => 'report_date']],
        ];
        yield 'time partitioning with expiration' => [
            ['type' => 'DAY', 'field' => 'report_date', 'expiration_ms' => 86400000],
            null,
            null,
            ['timePartitioning' => ['type' => 'DAY', 'field' => 'report_date', 'expirationMs' => 86400000]],
        ];
        yield 'time partitioning without field' => [
            ['type' => 'HOUR'],
            null,
            null,
            ['timePartitioning' => ['type' => 'HOUR']],
        ];
        yield 'range partitioning' => [
            null,
            self::RANGE_PARTITIONING,
            null,
            [
                'rangePartitioning' => [
                    'field' => 'id',
                    'range' => ['start' => 0, 'end' => 100, 'interval' => 10],
                ],
            ],
        ];
        yield 'clustering' => [
            null,
            null,
            self::CLUSTERING,
            ['clustering' => ['fields' => ['country', 'city']]],
        ];
        yield 'time partitioning and clustering' => [
            self::TIME_PARTITIONING,
            null,
            self::CLUSTERING,
            [
                'timePartitioning' => ['type' => 'DAY', 'field' => 'report_date'],
                'clustering' => ['fields' => ['country', 'city']],
            ],
        ];
    }

    public function testGetConfiguredKeys(): void
    {
        self::assertSame([], (new MappingTableLayout(null, null, null))->getConfiguredKeys());
        self::assertSame(
            ['time_partitioning', 'clustering'],
            (new MappingTableLayout(self::TIME_PARTITIONING, null, self::CLUSTERING))->getConfiguredKeys(),
        );
        self::assertSame(
            ['time_partitioning', 'range_partitioning', 'clustering'],
            (new MappingTableLayout(self::TIME_PARTITIONING, self::RANGE_PARTITIONING, self::CLUSTERING))
                ->getConfiguredKeys(),
        );
    }

    /**
     * @dataProvider differencesProvider
     */
    public function testFindDifferences(MappingTableLayout $layout, array $storedDefinition, array $expected): void
    {
        $table = new TableInfo(['id' => 'in.c-main.table', 'definition' => $storedDefinition]);

        self::assertSame($expected, $layout->findDifferences($table));
    }

    public function differencesProvider(): Generator
    {
        yield 'same layout' => [
            new MappingTableLayout(self::TIME_PARTITIONING, null, self::CLUSTERING),
            [
                'timePartitioning' => ['type' => 'DAY', 'field' => 'report_date'],
                'clustering' => ['fields' => ['country', 'city']],
            ],
            [],
        ];
        yield 'different time field' => [
            new MappingTableLayout(self::TIME_PARTITIONING, null, null),
            ['timePartitioning' => ['type' => 'DAY', 'field' => 'created_at']],
            ['time_partitioning'],
        ];
        yield 'type differs only in case' => [
            new MappingTableLayout(['type' => 'day', 'field' => 'report_date'], null, null),
            ['timePartitioning' => ['type' => 'DAY', 'field' => 'report_date']],
            [],
        ];
        yield 'different type' => [
            new MappingTableLayout(['type' => 'HOUR', 'field' => 'report_date'], null, null),
            ['timePartitioning' => ['type' => 'DAY', 'field' => 'report_date']],
            ['time_partitioning'],
        ];
        yield 'stored has no layout' => [
            new MappingTableLayout(self::TIME_PARTITIONING, null, self::CLUSTERING),
            [],
            ['time_partitioning', 'clustering'],
        ];
        yield 'stored layout the configuration does not mention' => [
            new MappingTableLayout(null, null, self::CLUSTERING),
            [
                'timePartitioning' => ['type' => 'DAY', 'field' => 'report_date'],
                'clustering' => ['fields' => ['country', 'city']],
            ],
            [],
        ];
        yield 'range numbers match stored strings' => [
            new MappingTableLayout(null, self::RANGE_PARTITIONING, null),
            [
                'rangePartitioning' => [
                    'field' => 'id',
                    'range' => ['start' => '0', 'end' => '100', 'interval' => '10'],
                ],
            ],
            [],
        ];
        yield 'different range interval' => [
            new MappingTableLayout(null, self::RANGE_PARTITIONING, null),
            [
                'rangePartitioning' => [
                    'field' => 'id',
                    'range' => ['start' => '0', 'end' => '100', 'interval' => '20'],
                ],
            ],
            ['range_partitioning'],
        ];
        yield 'clustering order differs' => [
            new MappingTableLayout(null, null, self::CLUSTERING),
            ['clustering' => ['fields' => ['city', 'country']]],
            ['clustering'],
        ];
        yield 'ingestion-time partitioning stored with empty field' => [
            new MappingTableLayout(['type' => 'DAY'], null, null),
            ['timePartitioning' => ['type' => 'DAY', 'field' => '']],
            [],
        ];
        yield 'expiration compared only when configured' => [
            new MappingTableLayout(self::TIME_PARTITIONING, null, null),
            ['timePartitioning' => ['type' => 'DAY', 'field' => 'report_date', 'expirationMs' => '1000']],
            [],
        ];
        yield 'different expiration' => [
            new MappingTableLayout(['type' => 'DAY', 'field' => 'report_date', 'expiration_ms' => 2000], null, null),
            ['timePartitioning' => ['type' => 'DAY', 'field' => 'report_date', 'expirationMs' => '1000']],
            ['time_partitioning'],
        ];
    }
}
