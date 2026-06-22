<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\DeferredTasks\TableDefinition;

use Keboola\OutputMapping\DeferredTasks\TableDefinition\TableDefinitionDescription;
use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;

class TableDefinitionDescriptionTest extends TestCase
{
    public function testHasChanges(): void
    {
        self::assertTrue((new TableDefinitionDescription('in.c-main.t', 'desc', []))->hasChanges());
        self::assertTrue((new TableDefinitionDescription('in.c-main.t', null, ['col' => 'd']))->hasChanges());
        self::assertFalse((new TableDefinitionDescription('in.c-main.t', null, []))->hasChanges());
    }

    public function testApplyTableAndColumnDescriptions(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::once())
            ->method('updateTableDefinition')
            ->with(
                'in.c-main.t',
                [
                    'description' => 'table desc',
                    'columns' => [
                        ['name' => 'col1', 'description' => 'col1 desc'],
                        ['name' => 'col2', 'description' => 'col2 desc'],
                    ],
                ],
            )
            ->willReturn([]);

        $description = new TableDefinitionDescription(
            'in.c-main.t',
            'table desc',
            ['col1' => 'col1 desc', 'col2' => 'col2 desc'],
        );
        $description->apply($client);
    }

    public function testApplyTableDescriptionOnly(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::once())
            ->method('updateTableDefinition')
            ->with('in.c-main.t', ['description' => 'table desc'])
            ->willReturn([]);

        (new TableDefinitionDescription('in.c-main.t', 'table desc', []))->apply($client);
    }

    public function testApplyColumnDescriptionsOnly(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::once())
            ->method('updateTableDefinition')
            ->with('in.c-main.t', ['columns' => [['name' => 'col1', 'description' => 'col1 desc']]])
            ->willReturn([]);

        (new TableDefinitionDescription('in.c-main.t', null, ['col1' => 'col1 desc']))->apply($client);
    }

    public function testApplyDoesNothingWhenEmpty(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::never())->method('updateTableDefinition');

        (new TableDefinitionDescription('in.c-main.t', null, []))->apply($client);
    }
}
