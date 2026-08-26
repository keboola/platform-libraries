<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Keboola\StagingProvider\Staging\StagingType;

class DownloadTablesS3DefaultTest extends AbstractTestCase
{
    #[NeedsTestTables(2)]
    public function testReadTablesS3DefaultBackend(): void
    {
        $clientWrapper = $this->initClient();
        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory(
                clientWrapper: $clientWrapper,
                logger:  $this->testLogger,
                stagingType: StagingType::S3,
            ),
        );
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
            new ReaderOptions(true),
        );

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test.csv.manifest');
        self::assertEquals($this->firstTableId, $manifest['id']);
        $this->assertS3info($manifest);

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.csv.manifest');
        self::assertEquals($this->secondTableId, $manifest['id']);
        $this->assertS3info($manifest);
        // info level: the unchanged, greppable contract messages
        self::assertTrue($this->testHandler->hasInfoThatContains('Processing 2 S3 table exports.'));
        self::assertTrue($this->testHandler->hasInfoThatContains(
            sprintf('Fetched table %s.', $this->firstTableId),
        ));
        self::assertTrue($this->testHandler->hasInfoThatContains('All tables were fetched.'));

        // debug level: all the timing and Storage job detail added by AJDA-3148
        self::assertTrue($this->testHandler->hasDebugThatContains('Queued 2 table exports in '));
        self::assertTrue($this->testHandler->hasDebugThatContains('Waiting for 2 storage jobs to finish.'));
        self::assertTrue($this->testHandler->hasDebugThatContains(
            sprintf('Fetched table %s. Export job ', $this->firstTableId),
        ));
        self::assertTrue($this->testHandler->hasDebugThatContains('Wrote 2 table manifests in '));
        self::assertTrue($this->testHandler->hasDebugThatContains('All tables were fetched. 2 tables in '));
    }

    #[NeedsTestTables(2)]
    public function testReadTablesABSUnsupportedBackend(): void
    {
        $clientWrapper = $this->initClient();
        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory(
                clientWrapper: $clientWrapper,
                stagingType: StagingType::Abs,
            ),
        );
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

        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage('This project does not have ABS backend.');
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            new ReaderOptions(true),
        );
    }
}
