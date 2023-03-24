<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Redshift;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Tests\Writer\CreateBranchTrait;
use Keboola\OutputMapping\Tests\Writer\Workspace\BaseWriterWorkspaceTest;
use Keboola\OutputMapping\Writer\TableWriter;

class RedshiftWriterWorkspaceTest extends BaseWriterWorkspaceTest
{
    use CreateBranchTrait;

    private const INPUT_BUCKET = 'in.c-RedshiftWriterWorkspaceTest';
    private const OUTPUT_BUCKET = 'out.c-RedshiftWriterWorkspaceTest';

    public function setUp(): void
    {
        parent::setUp();
        $this->clearBuckets([
            self::INPUT_BUCKET,
            self::OUTPUT_BUCKET,
        ]);
    }

    public function testRedshiftTableOutputMapping(): void
    {
        $factory = $this->getStagingFactory(
            null,
            'json',
            null,
            [AbstractStrategyFactory::WORKSPACE_REDSHIFT, 'redshift']
        );
        // initialize the workspace mock
        $factory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_REDSHIFT)
            ->getDataStorage()->getWorkspaceId();

        $root = $this->tmp->getTmpFolder();
        $this->prepareWorkspaceWithTables('redshift', 'RedshiftWriterWorkspaceTest');
        // snowflake bucket does not work - https://keboola.atlassian.net/browse/KBC-228
        $this->clientWrapper->getBasicClient()->createBucket('RedshiftWriterWorkspaceTest', 'out', '', 'redshift');
        $configs = [
            [
                'source' => 'table1a',
                'destination' => self::OUTPUT_BUCKET . '.table1a',
            ],
            [
                'source' => 'table2a',
                'destination' => self::OUTPUT_BUCKET . '.table2a',
            ],
        ];
        file_put_contents(
            $root . '/table1a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name']]
            )
        );
        file_put_contents(
            $root . '/table2a.manifest',
            json_encode(
                ['columns' => ['Id2', 'Name2']]
            )
        );
        $writer = new TableWriter($factory);

        $tableQueue = $writer->uploadTables(
            '/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::WORKSPACE_REDSHIFT,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(2, $jobIds);
        self::assertNotEmpty($jobIds[0]);
        self::assertNotEmpty($jobIds[1]);

        self::assertTablesExists(
            self::OUTPUT_BUCKET,
            [
                self::OUTPUT_BUCKET . '.table1a',
                self::OUTPUT_BUCKET . '.table2a',
            ]
        );
        self::assertTableRowsEquals(self::OUTPUT_BUCKET . '.table1a', [
            '"id","name"',
            '"test","test"',
            '"aabb","ccdd"',
        ]);
    }
}
