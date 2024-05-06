<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\SourceValidator;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use Keboola\OutputMapping\SourcesValidator\WorkspaceSourcesValidator;
use Keboola\OutputMapping\Writer\FileItem;
use PHPUnit\Framework\TestCase;

class WorkspaceSourcesValidatorTest extends TestCase
{
    public function testValidateManifestWithConfiguration(): void
    {
        $validator = new WorkspaceSourcesValidator(false);
        $manifests = [$this->createConfiguredMock(FileItem::class, ['getName' => 'valid.manifest'])];
        $configurationSource = [$this->createConfiguredMock(
            MappingFromRawConfiguration::class,
            ['getSourceName' => 'valid'],
        )];

        $validator->validateManifestWithConfiguration($manifests, $configurationSource);

        $this->assertTrue(true);
    }

    public function testValidateManifestWithConfigurationWithInvalidManifest(): void
    {
        $validator = new WorkspaceSourcesValidator(false);
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Table with manifests not found: "invalid"');

        $manifests = [$this->createConfiguredMock(FileItem::class, ['getName' => 'valid.manifest'])];
        $configurationSource = [$this->createConfiguredMock(
            MappingFromRawConfiguration::class,
            ['getSourceName' => 'invalid'],
        )];

        $validator->validateManifestWithConfiguration($manifests, $configurationSource);
    }

    public function testValidateManifestWithConfigurationWithInvalidManifestAndFailedJod(): void
    {
        $validator = new WorkspaceSourcesValidator(true);

        $manifests = [$this->createConfiguredMock(FileItem::class, ['getName' => 'valid.manifest'])];
        $configurationSource = [$this->createConfiguredMock(
            MappingFromRawConfiguration::class,
            ['getSourceName' => 'invalid'],
        )];

        $validator->validateManifestWithConfiguration($manifests, $configurationSource);

        $this->assertTrue(true);
    }
}
