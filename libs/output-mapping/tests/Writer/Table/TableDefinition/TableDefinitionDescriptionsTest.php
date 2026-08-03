<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinition;

use Keboola\Datatype\Definition\GenericStorage;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Storage\TableDescription;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinition;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionFromColumns;
use Keboola\OutputMapping\Writer\Table\TableDefinitionFromSchema\TableDefinitionFromSchema;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

/**
 * The create endpoint nests the column description in `columns[].definition.description`, unlike the update
 * endpoint which takes a flat `columns[].description` (see TableDescriptionModifier).
 */
class TableDefinitionDescriptionsTest extends TestCase
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

    public function testRequestDataWithoutDescriptionsIsUnchanged(): void
    {
        $tableDefinition = new TableDefinitionFromColumns('table', ['col1', 'col2'], []);

        self::assertSame(
            [
                'name' => 'table',
                'primaryKeysNames' => [],
                'columns' => [['name' => 'col1'], ['name' => 'col2']],
            ],
            $tableDefinition->getRequestData(),
        );
    }

    public function testEmptyDescriptionsAddNothing(): void
    {
        $tableDefinition = new TableDefinitionFromColumns('table', ['col1'], []);
        $tableDefinition->setDescriptions(new TableDescription(self::TABLE_ID, null, []), $this->logger);

        self::assertSame(
            [
                'name' => 'table',
                'primaryKeysNames' => [],
                'columns' => [['name' => 'col1']],
            ],
            $tableDefinition->getRequestData(),
        );
    }

    /**
     * A `definition` holding only a description carries no type, so the table stays non-typed.
     */
    public function testDescriptionsOnANonTypedDefinition(): void
    {
        $tableDefinition = new TableDefinitionFromColumns('table', ['col1', 'col2'], ['col1']);
        $tableDefinition->setDescriptions(
            new TableDescription(self::TABLE_ID, 'table desc', ['col1' => 'col1 desc']),
            $this->logger,
        );

        self::assertSame(
            [
                'name' => 'table',
                'primaryKeysNames' => ['col1'],
                'columns' => [
                    ['name' => 'col1', 'definition' => ['description' => 'col1 desc']],
                    ['name' => 'col2'],
                ],
                'description' => 'table desc',
            ],
            $tableDefinition->getRequestData(),
        );
    }

    public function testDescriptionOnATypedColumnKeepsTheType(): void
    {
        $tableDefinition = new TableDefinition(new TableDefinitionColumnFactory([], 'snowflake', false));
        $tableDefinition->setTableName('table');
        $tableDefinition->addColumn('col1', (new GenericStorage('varchar', ['length' => '25']))->toMetadata());
        $tableDefinition->addColumn('col2', (new GenericStorage('int'))->toMetadata());
        $tableDefinition->setDescriptions(
            new TableDescription(self::TABLE_ID, 'table desc', ['col1' => 'col1 desc']),
            $this->logger,
        );

        $requestData = $tableDefinition->getRequestData();

        self::assertSame('table desc', $requestData['description']);
        self::assertSame(
            ['name' => 'col1', 'basetype' => 'STRING', 'definition' => ['description' => 'col1 desc']],
            $requestData['columns'][0],
        );
        self::assertSame(['name' => 'col2', 'basetype' => 'INTEGER'], $requestData['columns'][1]);
    }

    public function testDescriptionsOnADefinitionFromSchema(): void
    {
        $tableDefinition = new TableDefinitionFromSchema(
            'table',
            [
                new MappingFromConfigurationSchemaColumn([
                    'name' => 'col1',
                    'data_type' => ['base' => ['type' => 'STRING']],
                ]),
            ],
            'snowflake',
        );
        $tableDefinition->setDescriptions(
            new TableDescription(self::TABLE_ID, 'table desc', ['col1' => 'col1 desc']),
            $this->logger,
        );

        $requestData = $tableDefinition->getRequestData();

        self::assertSame('table desc', $requestData['description']);
        self::assertSame('col1 desc', $requestData['columns'][0]['definition']['description']);
        self::assertSame('STRING', $requestData['columns'][0]['basetype']);
    }

    /**
     * A description of a column which is not part of the payload must never append a new column entry - that
     * would create a column the data does not have.
     */
    public function testDescriptionOfAnUnknownColumnIsSkippedWithWarning(): void
    {
        $tableDefinition = new TableDefinitionFromColumns('table', ['col1'], []);
        $tableDefinition->setDescriptions(
            new TableDescription(self::TABLE_ID, null, ['col1' => 'col1 desc', 'nope' => 'nope desc']),
            $this->logger,
        );

        self::assertSame(
            [
                'name' => 'table',
                'primaryKeysNames' => [],
                'columns' => [['name' => 'col1', 'definition' => ['description' => 'col1 desc']]],
            ],
            $tableDefinition->getRequestData(),
        );

        self::assertTrue($this->logHandler->hasWarningThatContains(sprintf(
            'Cannot store description of column(s) "nope" of table "%s", the column(s) do not exist.',
            self::TABLE_ID,
        )));
    }

    public function testKnownColumnsAreNotReportedAsMissing(): void
    {
        $tableDefinition = new TableDefinitionFromColumns('table', ['col1'], []);
        $tableDefinition->setDescriptions(
            new TableDescription(self::TABLE_ID, 'table desc', ['col1' => 'col1 desc']),
            $this->logger,
        );

        $tableDefinition->getRequestData();

        self::assertFalse($this->logHandler->hasWarningThatContains('Cannot store description of column(s)'));
    }
}
