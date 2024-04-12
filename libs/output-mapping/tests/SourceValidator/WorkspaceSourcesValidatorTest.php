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
    protected function setUp(): void
    {
        $this->validator = new WorkspaceSourcesValidator();
    }

    public function testValidateManifestWithConfiguration(): void
    {
        $manifests = [$this->createConfiguredMock(FileItem::class, ['getName' => 'valid.manifest'])];
        $configurationSource = [$this->createConfiguredMock(
            MappingFromRawConfiguration::class,
            ['getSourceName' => 'valid'],
        )];

        $this->validator->validateManifestWithConfiguration($manifests, $configurationSource);

        $this->assertTrue(true);
    }

    public function testValidateManifestWithConfigurationWithInvalidManifest(): void
    {
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Table with manifests not found: "invalid"');

        $manifests = [$this->createConfiguredMock(FileItem::class, ['getName' => 'valid.manifest'])];
        $configurationSource = [$this->createConfiguredMock(
            MappingFromRawConfiguration::class,
            ['getSourceName' => 'invalid'],
        )];

        $this->validator->validateManifestWithConfiguration($manifests, $configurationSource);
    }
}
