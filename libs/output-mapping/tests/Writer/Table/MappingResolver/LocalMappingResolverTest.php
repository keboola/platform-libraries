<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\MappingResolver;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Table\MappingResolver\LocalMappingResolver;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use Keboola\OutputMapping\Writer\Table\Source\LocalFileSource;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Psr\Log\Test\TestLogger;

class LocalMappingResolverTest extends TestCase
{
    public function testAccessors(): void
    {
        $resolver = new LocalMappingResolver('path', new NullLogger());

        self::assertNull($resolver->getSlicerInputSizeThreshold());

        $resolver->setSlicerInputSizeThreshold('1MB');
        self::assertSame('1MB', $resolver->getSlicerInputSizeThreshold());
    }

    public function testResolveMappingSources(): void
    {
        $prefix = 'data/in/tables';
        $temp = new Temp();

        $fileInfo = $temp->createFile(sprintf('%s/%s', $prefix, 'test.csv'));

        // sliced file
        $temp->createFile(sprintf('%s/mySource/%s', $prefix, 'part1'));
        $temp->createFile(sprintf('%s/mySource/%s', $prefix, 'part2'));
        $manifestFileInfo = $temp->createFile(sprintf('%s/mySource.manifest', $prefix));

        $resolver = new LocalMappingResolver($temp->getTmpFolder(), new NullLogger());
        $mappingSources = $resolver->resolveMappingSources(
            $prefix,
            [
                'mapping' => [
                    [
                        'source' => 'test.csv',
                    ],
                ],
            ],
            false,
            false,
        );

        usort($mappingSources, function (MappingSource $a, MappingSource $b) {
            return strcmp($a->getSourceName(), $b->getSourceName());
        });

        self::assertCount(2, $mappingSources);

        $slicedMappingSource = $mappingSources[0];
        self::assertSame('mySource', $slicedMappingSource->getSourceName());
        self::assertSame(null, $slicedMappingSource->getMapping());

        $source = $slicedMappingSource->getSource();
        self::assertInstanceOf(LocalFileSource::class, $source);

        self::assertSame('mySource', $source->getName());
        self::assertSame($temp->getTmpFolder() . '/data/in/tables/mySource', $source->getFile()->getPathname());
        self::assertTrue($source->isSliced());
        self::assertNotNull($slicedMappingSource->getManifestFile());
        self::assertSame($manifestFileInfo->getPathname(), $slicedMappingSource->getManifestFile()->getPathname());

        $mappingSource = $mappingSources[1];
        self::assertSame($fileInfo->getFilename(), $mappingSource->getSourceName());

        // check resolved that source is combined with mapping configuration
        self::assertSame(
            [
                'source' => 'test.csv',
            ],
            $mappingSource->getMapping(),
        );

        $source = $mappingSource->getSource();
        self::assertInstanceOf(LocalFileSource::class, $source);

        self::assertSame($fileInfo->getFilename(), $source->getName());
        self::assertSame($temp->getTmpFolder() . '/data/in/tables/test.csv', $source->getFile()->getPathname());
        self::assertFalse($source->isSliced());
        self::assertNull($mappingSource->getManifestFile());
    }

    public function testCombineSourcesWithMappingsFromConfiguration(): void
    {
        $prefix = 'data/in/tables';
        $temp = new Temp();

        $fileInfo = $temp->createFile(sprintf('%s/%s', $prefix, 'test.csv'));

        $mapping = [
            'mapping' => [
                [
                    'source' => 'mySource', // non-existiong source should produce error on non-failed job
                ],
                [
                    'source' => 'test.csv',
                ],
            ],
        ];

        $resolver = new LocalMappingResolver($temp->getTmpFolder(), new NullLogger());

        $mappingSources = $resolver->resolveMappingSources($prefix, $mapping, true, false);
        self::assertCount(1, $mappingSources);

        $mappingSource = $mappingSources[0];
        self::assertSame($fileInfo->getFilename(), $mappingSource->getSourceName());

        // check resolved that source is combined with mapping configuration
        self::assertSame(
            [
                'source' => 'test.csv',
            ],
            $mappingSource->getMapping(),
        );

        $source = $mappingSource->getSource();
        self::assertInstanceOf(LocalFileSource::class, $source);

        self::assertSame($fileInfo->getFilename(), $source->getName());
        self::assertSame($temp->getTmpFolder() . '/data/in/tables/test.csv', $source->getFile()->getPathname());
        self::assertFalse($source->isSliced());
        self::assertNull($mappingSource->getManifestFile());

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Table sources not found: "mySource"');
        $resolver->resolveMappingSources($prefix, $mapping, false, false);
    }
    public function testResolveMappingSourcesFailsIfOrphanedManifestPresents(): void
    {
        $prefix = 'data/in/tables';
        $temp = new Temp();

        $temp->createFile(sprintf('%s/mySource.manifest', $prefix));

        $resolver = new LocalMappingResolver($temp->getTmpFolder(), new NullLogger());

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Found orphaned table manifest: "mySource.manifest"');

        $resolver->resolveMappingSources(
            $prefix,
            [],
            false,
            false,
        );
    }

    public function testResolveMappingSourcesUsesSliceHelper(): void
    {
        $prefix = 'data/in/tables';
        $temp = new Temp();

        $csvFile = $temp->createFile(sprintf('%s/mySource.csv', $prefix));
        file_put_contents($csvFile->getPathname(), '"id","name"' . PHP_EOL . '"123","Test Name"');

        $logger = new TestLogger();

        $resolver = new LocalMappingResolver($temp->getTmpFolder(), $logger);
        $resolver->setSlicerInputSizeThreshold('10b');

        $mappingSources = $resolver->resolveMappingSources(
            $prefix,
            [],
            false,
            true,
        );

        self::assertCount(1, $mappingSources);

        $mappingSource = $mappingSources[0];
        self::assertNotNull($mappingSource->getManifestFile());
        self::assertFileExists($mappingSource->getManifestFile()->getPathname());

        self::assertCount(2, $logger->records);
        self::assertTrue($logger->hasInfo('Slicing table "mySource.csv".'));
        self::assertTrue($logger->hasInfoThatContains('Table "mySource.csv" sliced'));
    }
}
