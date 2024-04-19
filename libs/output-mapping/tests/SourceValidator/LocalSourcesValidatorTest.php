<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\SourceValidator;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use Keboola\OutputMapping\SourcesValidator\LocalSourcesValidator;
use Keboola\OutputMapping\Writer\FileItem;
use PHPUnit\Framework\TestCase;

class LocalSourcesValidatorTest extends TestCase
{
    private LocalSourcesValidator $localSourcesValidator;

    protected function setUp(): void
    {
        $this->localSourcesValidator = new LocalSourcesValidator(false);
    }

    public function testValidatePhysicalFilesWithManifestWithMissingTable(): void
    {
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Table sources not found: "missing.table"');

        $dataItems = [$this->createMock(FileItem::class)];
        $manifests = [$this->createConfiguredMock(FileItem::class, ['getName' => 'missing.table.manifest'])];

        $this->localSourcesValidator->validatePhysicalFilesWithManifest($dataItems, $manifests);
    }

    public function testValidatePhysicalFilesWithManifestWithOrphanedManifest(): void
    {
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Found orphaned table manifest: "orphaned.manifest"');

        $dataItems = [$this->createMock(FileItem::class)];
        $manifests = [$this->createConfiguredMock(FileItem::class, ['getName' => 'orphaned.manifest'])];

        $this->localSourcesValidator->validatePhysicalFilesWithManifest($dataItems, $manifests);
    }

    public function testValidatePhysicalFilesWithConfigurationWithMissingSource(): void
    {
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Table sources not found: "missing.source"');

        $dataItems = [$this->createMock(FileItem::class)];
        $configurationSource = [$this->createConfiguredMock(
            MappingFromRawConfiguration::class,
            ['getSourceName' => 'missing.source'],
        )];

        $this->localSourcesValidator->validatePhysicalFilesWithConfiguration($dataItems, $configurationSource);
    }

    public function testValidatePhysicalFilesWithConfigurationWithFailedJob(): void
    {
        $localSourcesValidator = new LocalSourcesValidator(true);

        $dataItems = [$this->createMock(FileItem::class)];
        $configurationSource = [$this->createConfiguredMock(
            MappingFromRawConfiguration::class,
            ['getSourceName' => 'missing.source'],
        )];

        $localSourcesValidator->validatePhysicalFilesWithConfiguration($dataItems, $configurationSource);

        $this->assertTrue(true);
    }
}
