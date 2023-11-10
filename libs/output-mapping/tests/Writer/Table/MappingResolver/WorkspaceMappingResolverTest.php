<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\MappingResolver;

use Keboola\OutputMapping\Writer\Table\MappingResolver\WorkspaceMappingResolver;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSourceFactoryInterface;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;

class WorkspaceMappingResolverTest extends TestCase
{
    public function testResolveMappingSources(): void
    {
        $prefix = 'data/in/tables';
        $temp = new Temp();

        $workspaceItemSourceFactory = $this->createMock(WorkspaceItemSourceFactoryInterface::class);
        $workspaceItemSourceFactory->expects(self::exactly(2))
            ->method('createSource')
            ->willReturnCallback(
                function (string $sourcePathPrefix, string $sourceName): WorkspaceItemSource {
                    return new WorkspaceItemSource(
                        $sourceName,
                        '123456',
                        $sourceName,
                        false,
                    );
                },
            )
        ;

        $manifestFileInfo = $temp->createFile(sprintf('%s/mySource.manifest', $prefix));

        $resolver = new WorkspaceMappingResolver($temp->getTmpFolder(), $workspaceItemSourceFactory);
        $mappingSources = $resolver->resolveMappingSources(
            $prefix,
            [
                'mapping' => [
                    [
                        'source' => 'table1',
                    ],
                ],
            ],
            false,
        );

        self::assertCount(2, $mappingSources);

        $mappingSourceFromMapping = $mappingSources[0];

        // check resolved that source is combined with mapping configuration
        self::assertSame(
            [
                'source' => 'table1',
            ],
            $mappingSourceFromMapping->getMapping(),
        );
        self::assertNull($mappingSourceFromMapping->getManifestFile());

        $source = $mappingSourceFromMapping->getSource();
        self::assertInstanceOf(WorkspaceItemSource::class, $source);

        self::assertSame('table1', $source->getName());
        self::assertSame('123456', $source->getWorkspaceId());
        self::assertSame('table1', $source->getDataObject());
        self::assertFalse($source->isSliced());

        $mappingSource = $mappingSources[1];
        self::assertNull($mappingSource->getMapping());
        self::assertNotNull($mappingSource->getManifestFile());
        self::assertSame($manifestFileInfo->getPathname(), $mappingSource->getManifestFile()->getPathname());

        $source = $mappingSource->getSource();
        self::assertInstanceOf(WorkspaceItemSource::class, $source);

        self::assertSame('mySource', $source->getName());
        self::assertSame('123456', $source->getWorkspaceId());
        self::assertSame('mySource', $source->getDataObject());
        self::assertFalse($source->isSliced());
    }
}
