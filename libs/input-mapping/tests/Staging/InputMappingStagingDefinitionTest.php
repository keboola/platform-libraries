<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Staging;

use Keboola\InputMapping\Exception\StagingException;
use Keboola\InputMapping\File\Strategy\Local as LocalFileStrategy;
use Keboola\InputMapping\Staging\AbstractStagingDefinition;
use Keboola\InputMapping\Staging\FileStagingInterface;
use Keboola\InputMapping\Staging\InputMappingStagingDefinition;
use Keboola\InputMapping\Staging\WorkspaceStagingInterface;
use Keboola\InputMapping\Table\Strategy\Local as LocalTableStrategy;
use PHPUnit\Framework\TestCase;

class InputMappingStagingDefinitionTest extends TestCase
{
    public function testAccessors(): void
    {
        $definition = new InputMappingStagingDefinition('foo', LocalFileStrategy::class, LocalTableStrategy::class);
        self::assertSame('foo', $definition->getName());
        self::assertSame(LocalFileStrategy::class, $definition->getFileStagingClass());
        self::assertSame(LocalTableStrategy::class, $definition->getTableStagingClass());
        self::assertNull($definition->getFileDataStaging());
        self::assertNull($definition->getFileMetadataStaging());
        self::assertNull($definition->getTableDataStaging());
        self::assertNull($definition->getTableMetadataStaging());
        $definition->setFileDataStaging($this->createMock(FileStagingInterface::class));
        $definition->setFileMetadataStaging($this->createMock(FileStagingInterface::class));
        $definition->setTableDataStaging($this->createMock(WorkspaceStagingInterface::class));
        $definition->setTableMetadataStaging($this->createMock(WorkspaceStagingInterface::class));
        self::assertNotNull($definition->getFileDataStaging());
        self::assertNotNull($definition->getFileMetadataStaging());
        self::assertNotNull($definition->getTableDataStaging());
        self::assertNotNull($definition->getTableMetadataStaging());
    }

    public function testFileValidationInvalidData(): void
    {
        $definition = new InputMappingStagingDefinition(
            'foo',
            LocalFileStrategy::class,
            LocalTableStrategy::class,
            null,
            $this->createMock(FileStagingInterface::class),
            null,
            null,
        );
        $this->expectException(StagingException::class);
        $this->expectExceptionMessage('Undefined file data provider in "foo" staging.');
        $definition->validateFor(AbstractStagingDefinition::STAGING_FILE);
    }

    public function testFileValidationInvalidMetadata(): void
    {
        $definition = new InputMappingStagingDefinition(
            'foo',
            LocalFileStrategy::class,
            LocalTableStrategy::class,
            $this->createMock(FileStagingInterface::class),
            null,
            null,
            null,
        );
        $this->expectException(StagingException::class);
        $this->expectExceptionMessage('Undefined file metadata provider in "foo" staging.');
        $definition->validateFor(AbstractStagingDefinition::STAGING_FILE);
    }

    public function testFileValidation(): void
    {
        $definition = new InputMappingStagingDefinition(
            'foo',
            LocalFileStrategy::class,
            LocalTableStrategy::class,
            $this->createMock(FileStagingInterface::class),
            $this->createMock(FileStagingInterface::class),
            null,
            null,
        );
        $definition->validateFor(AbstractStagingDefinition::STAGING_FILE);
        self::assertTrue(true);
    }

    public function testTableValidationInvalidData(): void
    {
        $definition = new InputMappingStagingDefinition(
            'foo',
            LocalFileStrategy::class,
            LocalTableStrategy::class,
            null,
            null,
            null,
            $this->createMock(WorkspaceStagingInterface::class),
        );
        $this->expectException(StagingException::class);
        $this->expectExceptionMessage('Undefined table data provider in "foo" staging.');
        $definition->validateFor(AbstractStagingDefinition::STAGING_TABLE);
    }

    public function testTableValidationInvalidMetadata(): void
    {
        $definition = new InputMappingStagingDefinition(
            'foo',
            LocalFileStrategy::class,
            LocalTableStrategy::class,
            null,
            null,
            $this->createMock(WorkspaceStagingInterface::class),
            null,
        );
        $this->expectException(StagingException::class);
        $this->expectExceptionMessage('Undefined table metadata provider in "foo" staging.');
        $definition->validateFor(AbstractStagingDefinition::STAGING_TABLE);
    }

    public function testTableValidation(): void
    {
        $definition = new InputMappingStagingDefinition(
            'foo',
            LocalFileStrategy::class,
            LocalTableStrategy::class,
            null,
            null,
            $this->createMock(WorkspaceStagingInterface::class),
            $this->createMock(WorkspaceStagingInterface::class),
        );
        $definition->validateFor(AbstractStagingDefinition::STAGING_TABLE);
        self::assertTrue(true);
    }

    public function testTableValidationInvalidStagingType(): void
    {
        $definition = new InputMappingStagingDefinition(
            'foo',
            LocalFileStrategy::class,
            LocalTableStrategy::class,
            null,
            null,
            $this->createMock(WorkspaceStagingInterface::class),
            null,
        );
        $this->expectException(StagingException::class);
        $this->expectExceptionMessage('Unknown staging type: "invalid".');
        $definition->validateFor('invalid');
    }
}
