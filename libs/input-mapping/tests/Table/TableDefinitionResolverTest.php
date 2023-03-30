<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table;

use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\TableDefinitionResolver;
use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Throwable;

class TableDefinitionResolverTest extends TestCase
{
    private function getSingleTableSearchOptionsList(): InputTableOptionsList
    {
        return new InputTableOptionsList(
            [
                [
                    'source_search' => [
                        'key' => 'bdm.scaffold.tag',
                        'value' => 'test_table',
                    ],
                    'destination' => 'test',
                ],
            ]
        );
    }

    public function testResolveNoTableFound(): void
    {
        $client = self::createMock(Client::class);
        $client->method('searchTables')->willReturn([]);
        $resolver = new TableDefinitionResolver($client, new NullLogger());

        $this->expectException(Throwable::class);
        $this->expectExceptionMessage(
            'Table with metadata key: "bdm.scaffold.tag" and value: "test_table" was not found.'
        );
        $resolver->resolve($this->getSingleTableSearchOptionsList());
    }

    public function testResolveMoreThanOneTableFound(): void
    {
        $client = self::createMock(Client::class);
        $client->method('searchTables')->willReturn([
            [
                'id' => 'table1',
            ],
            [
                'id' => 'table1',
            ],
        ]);
        $resolver = new TableDefinitionResolver($client, new NullLogger());

        $this->expectException(Throwable::class);
        $this->expectExceptionMessage(
            'More than one table with metadata key: "bdm.scaffold.tag" ' .
            'and value: "test_table" was found: table1,table1.'
        );
        $resolver->resolve($this->getSingleTableSearchOptionsList());
    }

    public function testResolveTableFound(): void
    {
        $client = self::createMock(Client::class);
        $client->method('searchTables')->willReturn([
            [
                'id' => 'table1',
            ],
        ]);
        $resolver = new TableDefinitionResolver($client, new NullLogger());

        $result = $resolver->resolve($this->getSingleTableSearchOptionsList());
        self::assertInstanceOf(InputTableOptionsList::class, $result);
        self::assertSame([
            'source_search' => [
                'key' => 'bdm.scaffold.tag',
                'value' => 'test_table',
            ],
            'destination' => 'test',
            'columns' => [],
            'column_types' => [],
            'where_values' => [],
            'where_operator' => 'eq',
            'overwrite' => false,
            'use_view' => false,
            'keep_internal_timestamp_column' => true,
            'source' => 'table1',
        ], $result->getTables()[0]->getDefinition());
    }
}
