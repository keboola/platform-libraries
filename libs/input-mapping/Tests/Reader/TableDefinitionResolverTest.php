<?php

namespace Keboola\InputMapping\Tests\Reader;

use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\InputMapping\Reader\TableDefinitionResolver;
use Keboola\StorageApi\Client;

class TableDefinitionResolverTest extends \PHPUnit_Framework_TestCase
{
    private function getSingleTableSearchOptionsList()
    {
        return new InputTableOptionsList(
            [
                [
                    "search_source" => [
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
        $resolver = new TableDefinitionResolver($client);

        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Table with metadata key: bdm.scaffold.tag and value: test_table was not found');
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
        $resolver = new TableDefinitionResolver($client);

        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('More than one table with metadata key: bdm.scaffold.tag and value: test_table was not found: table1,table1');
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
        $resolver = new TableDefinitionResolver($client);

        $result = $resolver->resolve($this->getSingleTableSearchOptionsList());
        $this->assertInstanceOf(InputTableOptionsList::class, $result);
        $this->assertSame([
            "search_source" => [
                "key" => "bdm.scaffold.tag",
                "value" => "test_table",
                "search_by" => "table"
            ],
            "destination" => "test",
            'columns' => [],
            'where_values' => [],
            'where_operator' => 'eq',
            "source" => "table1",
        ], $result->getTables()[0]->getDefinition());
    }
}
