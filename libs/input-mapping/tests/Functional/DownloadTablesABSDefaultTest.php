<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Psr\Log\Test\TestLogger;

class DownloadTablesABSDefaultTest extends AbstractTestCase
{
    #[NeedsTestTables(2)]
    public function testReadTablesABSDefaultBackend(): void
    {
        $logger = new TestLogger();
        $reader = new Reader($this->getLocalStagingFactory(logger: $logger));
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test.csv',
            ],
            [
                'source' => $this->secondTableId,
                'destination' => 'test2.csv',
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::ABS,
            new ReaderOptions(true)
        );

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test.csv.manifest');
        self::assertEquals($this->firstTableId, $manifest['id']);
        $this->assertABSinfo($manifest);

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.csv.manifest');
        self::assertEquals($this->secondTableId, $manifest['id']);
        $this->assertABSinfo($manifest);

        self::assertTrue($logger->hasInfoThatContains('Processing 2 ABS table exports.'));
    }

    #[NeedsTestTables]
    public function testReadTablesS3UnsupportedBackend(): void
    {
        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test.csv',
            ],
        ]);

        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage('This project does not have S3 backend.');
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::S3,
            new ReaderOptions(true)
        );
    }
}
