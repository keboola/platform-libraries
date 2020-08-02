<?php

namespace Keboola\InputMapping\Tests\Reader;

use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\InputMapping\Reader\TableDefinitionResolver;
use Keboola\StorageApi\Client;
use Psr\Log\NullLogger;

class TableDefinitionResolverTest extends \PHPUnit_Framework_TestCase
{
    private function getSingleTableSearchOptionsList()
    {
        return new InputTableOptionsList(
            [
                [
                    "source_search" => [
                        "key" => "bdm.scaffold.tag",
                        "value" => "test_table",
                    ],
                    "destination" => "test",
                ],
            ]
        );
    }

    public function testResolveNoTableFound()
    {
        /** @var Client|\PHPUnit_Framework_MockObject_MockObject $client */
        $client = $this->createMock(Client::class);
        $client->method('searchTables')->willReturn([]);
        $resolver = new TableDefinitionResolver($client, new NullLogger);

        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Table with metadata key: "bdm.scaffold.tag" and value: "test_table" was not found.');
        $resolver->resolve($this->getSingleTableSearchOptionsList());
    }

    public function testResolveMoreThanOneTableFound()
    {
        /** @var Client|\PHPUnit_Framework_MockObject_MockObject $client */
        $client = $this->createMock(Client::class);
        $client->method('searchTables')->willReturn([
            [
                'id' => 'table1',
            ],
            [
                'id' => 'table1',
            ],
        ]);
        $resolver = new TableDefinitionResolver($client, new NullLogger);

        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('More than one table with metadata key: "bdm.scaffold.tag" and value: "test_table" was found: table1,table1.');
        $resolver->resolve($this->getSingleTableSearchOptionsList());
    }

    public function testResolveTableFound()
    {
        /** @var Client|\PHPUnit_Framework_MockObject_MockObject $client */
        $client = $this->createMock(Client::class);
        $client->method('searchTables')->willReturn([
            [
                'id' => 'table1',
            ],
        ]);
        $resolver = new TableDefinitionResolver($client, new NullLogger);

        $result = $resolver->resolve($this->getSingleTableSearchOptionsList());
        $this->assertInstanceOf(InputTableOptionsList::class, $result);
        $this->assertSame([
            "source_search" => [
                "key" => "bdm.scaffold.tag",
                "value" => "test_table"
            ],
            "destination" => "test",
            'columns' => [],
            'column_types' => [],
            'where_values' => [],
            'where_operator' => 'eq',
            "source" => "table1",
        ], $result->getTables()[0]->getDefinition());
    }
}
