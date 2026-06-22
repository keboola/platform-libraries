<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Storage\TableDescriptionUpdater;
use Keboola\OutputMapping\Storage\TableInfo;
use Keboola\StorageApi\Client;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;

class TableDescriptionUpdaterTest extends TestCase
{
    private function tableInfo(array $metadata = [], array $columnMetadata = []): TableInfo
    {
        return new TableInfo([
            'id' => 'in.c-main.table',
            'isTyped' => false,
            'primaryKey' => [],
            'columns' => ['col1', 'col2'],
            'metadata' => $metadata,
            'columnMetadata' => $columnMetadata,
        ]);
    }

    private function clientWrapper(Client $client): ClientWrapper
    {
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($client);
        return $clientWrapper;
    }

    private function source(?string $tableDescription, array $columnDescriptions): MappingFromProcessedConfiguration
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getTableDescription')->willReturn($tableDescription);
        $source->method('getColumnDescriptions')->willReturn($columnDescriptions);
        return $source;
    }

    public function testUpdatesTableAndColumnDescriptionsWhenNoUserOverride(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::once())
            ->method('updateTableDefinition')
            ->with(
                'in.c-main.table',
                [
                    'description' => 'table desc',
                    'columns' => [
                        ['name' => 'col1', 'description' => 'col1 desc'],
                    ],
                ],
            )
            ->willReturn([]);

        $updater = new TableDescriptionUpdater($this->clientWrapper($client));
        $updater->updateDescriptions(
            $this->tableInfo(),
            $this->source('table desc', ['col1' => 'col1 desc']),
        );
    }

    public function testSkipsTableDescriptionSetByUser(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::once())
            ->method('updateTableDefinition')
            ->with(
                'in.c-main.table',
                ['columns' => [['name' => 'col1', 'description' => 'col1 desc']]],
            )
            ->willReturn([]);

        $updater = new TableDescriptionUpdater($this->clientWrapper($client));
        $updater->updateDescriptions(
            $this->tableInfo(
                metadata: [['key' => 'KBC.description', 'value' => 'user table desc', 'provider' => 'user']],
            ),
            $this->source('table desc', ['col1' => 'col1 desc']),
        );
    }

    public function testSkipsColumnDescriptionSetByUser(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::once())
            ->method('updateTableDefinition')
            ->with('in.c-main.table', ['description' => 'table desc'])
            ->willReturn([]);

        $updater = new TableDescriptionUpdater($this->clientWrapper($client));
        $updater->updateDescriptions(
            $this->tableInfo(
                columnMetadata: [
                    'col1' => [['key' => 'KBC.description', 'value' => 'user col1', 'provider' => 'user']],
                ],
            ),
            $this->source('table desc', ['col1' => 'col1 desc']),
        );
    }

    public function testComponentMetadataDoesNotBlockUpdate(): void
    {
        // a KBC.description set by the component itself (non-user provider) must not block the update
        $client = $this->createMock(Client::class);
        $client->expects(self::once())
            ->method('updateTableDefinition')
            ->with('in.c-main.table', ['description' => 'table desc'])
            ->willReturn([]);

        $updater = new TableDescriptionUpdater($this->clientWrapper($client));
        $updater->updateDescriptions(
            $this->tableInfo(
                metadata: [['key' => 'KBC.description', 'value' => 'old', 'provider' => 'keboola.my-component']],
            ),
            $this->source('table desc', []),
        );
    }

    public function testDoesNothingWhenNoDescriptions(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::never())->method('updateTableDefinition');

        $updater = new TableDescriptionUpdater($this->clientWrapper($client));
        $updater->updateDescriptions($this->tableInfo(), $this->source(null, []));
    }

    public function testDoesNothingWhenEverythingIsUserOwned(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::never())->method('updateTableDefinition');

        $updater = new TableDescriptionUpdater($this->clientWrapper($client));
        $updater->updateDescriptions(
            $this->tableInfo(
                metadata: [['key' => 'KBC.description', 'value' => 'u', 'provider' => 'user']],
                columnMetadata: [
                    'col1' => [['key' => 'KBC.description', 'value' => 'u', 'provider' => 'user']],
                ],
            ),
            $this->source('table desc', ['col1' => 'col1 desc']),
        );
    }
}
