<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\Strategy;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use Keboola\OutputMapping\SourcesValidator\WorkspaceSourcesValidator;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Writer\FileItem;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;
use Keboola\OutputMapping\Writer\Table\StrategyInterface;
use Throwable;

abstract class AbstractWorkspaceTableStrategyTestCase extends AbstractTestCase
{
    protected StrategyInterface $strategy;

    public function testPrepareLoadTaskOptions(): void
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getItemSourceClass')->willReturn(WorkspaceItemSource::class);
        $source->method('getWorkspaceId')->willReturn('123455');
        $source->method('getDataObject')->willReturn('987655');

        self::assertEquals(
            [
                'dataWorkspaceId' => '123455',
                'dataObject' => '987655',
            ],
            $this->strategy->prepareLoadTaskOptions($source),
        );
    }

    public function testListManifests(): void
    {
        for ($i = 0; $i < 3; $i++) {
            $manifestFile = $this->temp->getTmpFolder() . '/file_' . $i . '.csv.manifest';
            file_put_contents($manifestFile, '');
        }

        $result = $this->strategy->listManifests('/');

        $this->assertIsArray($result);
        self::assertCount(3, $result);
    }

    public function testInvalidListManifest(): void
    {
        $this->expectException(OutputOperationException::class);
        $this->expectExceptionMessage(sprintf(
            'Failed to list files: "The "%s/dir-unexist" directory does not exist.".',
            $this->temp->getTmpFolder(),
        ));
        $this->strategy->listManifests('/dir-unexist');
    }

    public function testListSources(): void
    {
        $configurations = [];
        for ($i = 0; $i < 3; $i++) {
            $configurations[] = new MappingFromRawConfiguration([
                'source' => 'source_' . $i,
                'destination' => 'destination_' . $i,
                'columns' => ['col1', 'col2'],
            ]);
        }

        $result = $this->strategy->listSources('/', $configurations);

        self::assertIsArray($result);
        self::assertCount(3, $result);
        foreach ($result as $source) {
            self::assertInstanceOf(WorkspaceItemSource::class, $source);
        }
    }

    public function testReadFileManifest(): void
    {
        $manifestFile = $this->temp->getTmpFolder() . '/file.csv.manifest';
        file_put_contents($manifestFile, json_encode([
            'columns' => [
                'col1',
                'col2',
            ],
        ]));

        $result = $this->strategy->readFileManifest(
            new FileItem('file.csv.manifest', '', 'file.csv.manifest', false),
        );

        self::assertIsArray($result);
        self::assertCount(2, $result['columns']);
    }

    public function testReadInvalidFileManifest(): void
    {
        $manifestFile = $this->temp->getTmpFolder() . '/file.csv.manifest';
        file_put_contents($manifestFile, 'invalidJson');

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage(sprintf(
            'Failed to parse manifest file "%s//file.csv.manifest" as "json": Syntax error',
            $this->temp->getTmpFolder(),
        ));
        $this->strategy->readFileManifest(
            new FileItem('file.csv.manifest', '', 'file.csv.manifest', false),
        );
    }

    public function testReadFileManifestNotFound(): void
    {
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('File \'' . $this->temp->getTmpFolder() . '//file.csv.manifest\' not found.');
        $this->strategy->readFileManifest(
            new FileItem('file.csv.manifest', '', 'file.csv.manifest', false),
        );
    }

    public function testGetSourcesValidator(): void
    {
        $result = $this->strategy->getSourcesValidator();
        $this->assertInstanceOf(WorkspaceSourcesValidator::class, $result);
    }

    public function testHasSlicerAlwaysFalse(): void
    {
        $result = $this->strategy->hasSlicer();
        $this->assertFalse($result);
    }

    public function testSliceFiles(): void
    {
        $this->expectException(Throwable::class);
        $this->expectExceptionMessage('Not implemented');
        $this->strategy->sliceFiles([], 'none');
    }
}
