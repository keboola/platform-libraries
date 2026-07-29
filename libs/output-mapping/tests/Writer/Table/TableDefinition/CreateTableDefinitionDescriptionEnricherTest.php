<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinition;

use Generator;
use Keboola\OutputMapping\Storage\TableDescription;
use Keboola\OutputMapping\Writer\Table\TableDefinition\CreateTableDefinitionDescriptionEnricher;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionFromColumns;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

class CreateTableDefinitionDescriptionEnricherTest extends TestCase
{
    private const TABLE_ID = 'in.c-main.table';

    private TestHandler $logHandler;
    private Logger $logger;

    protected function setUp(): void
    {
        parent::setUp();

        $this->logHandler = new TestHandler();
        $this->logger = new Logger('test', [$this->logHandler]);
    }

    public function enrichProvider(): Generator
    {
        yield 'table description only' => [
            'requestData' => [
                'name' => 'table',
                'primaryKeysNames' => [],
                'columns' => [['name' => 'col1']],
            ],
            'descriptions' => new TableDescription(self::TABLE_ID, 'table desc', []),
            'expectedRequestData' => [
                'name' => 'table',
                'primaryKeysNames' => [],
                'columns' => [['name' => 'col1']],
                'description' => 'table desc',
            ],
        ];

        yield 'column description on a typed column keeps the type' => [
            'requestData' => [
                'name' => 'table',
                'primaryKeysNames' => [],
                'columns' => [
                    [
                        'name' => 'col1',
                        'definition' => ['type' => 'VARCHAR', 'nullable' => true],
                        'basetype' => 'STRING',
                    ],
                    ['name' => 'col2', 'definition' => ['type' => 'NUMBER']],
                ],
            ],
            'descriptions' => new TableDescription(self::TABLE_ID, null, ['col1' => 'col1 desc']),
            'expectedRequestData' => [
                'name' => 'table',
                'primaryKeysNames' => [],
                'columns' => [
                    [
                        'name' => 'col1',
                        'definition' => [
                            'type' => 'VARCHAR',
                            'nullable' => true,
                            'description' => 'col1 desc',
                        ],
                        'basetype' => 'STRING',
                    ],
                    ['name' => 'col2', 'definition' => ['type' => 'NUMBER']],
                ],
            ],
        ];

        yield 'column description on a column without definition' => [
            'requestData' => [
                'name' => 'table',
                'primaryKeysNames' => ['col1'],
                'columns' => [
                    ['name' => 'col1'],
                    ['name' => 'col2'],
                ],
            ],
            'descriptions' => new TableDescription(
                self::TABLE_ID,
                'table desc',
                ['col1' => 'col1 desc', 'col2' => 'col2 desc'],
            ),
            'expectedRequestData' => [
                'name' => 'table',
                'primaryKeysNames' => ['col1'],
                'columns' => [
                    // only a description, no type - the table stays non-typed
                    ['name' => 'col1', 'definition' => ['description' => 'col1 desc']],
                    ['name' => 'col2', 'definition' => ['description' => 'col2 desc']],
                ],
                'description' => 'table desc',
            ],
        ];

        yield 'empty descriptions leave the payload untouched' => [
            'requestData' => [
                'name' => 'table',
                'primaryKeysNames' => [],
                'columns' => [['name' => 'col1']],
            ],
            'descriptions' => new TableDescription(self::TABLE_ID, null, []),
            'expectedRequestData' => [
                'name' => 'table',
                'primaryKeysNames' => [],
                'columns' => [['name' => 'col1']],
            ],
        ];
    }

    /** @dataProvider enrichProvider */
    public function testEnrich(
        array $requestData,
        TableDescription $descriptions,
        array $expectedRequestData,
    ): void {
        $enricher = new CreateTableDefinitionDescriptionEnricher($this->logger);

        self::assertSame($expectedRequestData, $enricher->enrich($requestData, $descriptions));
        self::assertFalse($this->logHandler->hasWarningRecords());
    }

    public function testDescriptionOfUnknownColumnIsSkippedAndLogged(): void
    {
        $enricher = new CreateTableDefinitionDescriptionEnricher($this->logger);

        $requestData = $enricher->enrich(
            [
                'name' => 'table',
                'primaryKeysNames' => [],
                'columns' => [['name' => 'col1']],
            ],
            new TableDescription(
                self::TABLE_ID,
                null,
                ['col1' => 'col1 desc', 'unknown1' => 'unknown1 desc', 'unknown2' => 'unknown2 desc'],
            ),
        );

        // a column which is not part of the payload must never be appended, it does not exist in the data
        self::assertSame(
            [
                'name' => 'table',
                'primaryKeysNames' => [],
                'columns' => [['name' => 'col1', 'definition' => ['description' => 'col1 desc']]],
            ],
            $requestData,
        );
        self::assertTrue($this->logHandler->hasWarningThatContains(sprintf(
            'Cannot store description of column(s) "unknown1", "unknown2" of table "%s", '
            . 'the column(s) do not exist.',
            self::TABLE_ID,
        )));
    }

    /**
     * The payload of a non-typed table must stay a JSON array of objects, so the enrichment must not turn
     * the column list into a JSON object.
     */
    public function testEnrichedNonTypedPayloadKeepsColumnsAsJsonArray(): void
    {
        $enricher = new CreateTableDefinitionDescriptionEnricher($this->logger);

        $requestData = $enricher->enrich(
            (new TableDefinitionFromColumns('table', ['Id', 'Name'], ['Id']))->getRequestData(),
            new TableDescription(self::TABLE_ID, 'table desc', ['Name' => 'Name desc']),
        );

        self::assertSame(
            '[{"name":"Id"},{"name":"Name","definition":{"description":"Name desc"}}]',
            json_encode($requestData['columns']),
        );
    }
}
