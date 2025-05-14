<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Staging;

use Keboola\InputMapping\Exception\StagingException;
use Keboola\InputMapping\File\Strategy\Local as LocalFile;
use Keboola\InputMapping\Staging\AbstractStagingDefinition;
use Keboola\InputMapping\Staging\InputMappingStagingDefinition;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Table\Strategy\Local as LocalTable;
use PHPUnit\Framework\TestCase;

class InputMappingStagingDefinitionTest extends TestCase
{
    public function testAccessors(): void
    {
        $definition = new InputMappingStagingDefinition('foo', LocalFile::class, LocalTable::class);
        self::assertSame('foo', $definition->getName());
        self::assertSame(LocalFile::class, $definition->getFileStagingClass());
        self::assertSame(LocalTable::class, $definition->getTableStagingClass());
        self::assertNull($definition->getFileDataProvider());
        self::assertNull($definition->getFileMetadataProvider());
        self::assertNull($definition->getTableDataProvider());
        self::assertNull($definition->getTableMetadataProvider());
        $definition->setFileDataProvider(new NullProvider());
        $definition->setFileMetadataProvider(new NullProvider());
        $definition->setTableDataProvider(new NullProvider());
        $definition->setTableMetadataProvider(new NullProvider());
        self::assertNotNull($definition->getFileDataProvider());
        self::assertNotNull($definition->getFileMetadataProvider());
        self::assertNotNull($definition->getTableDataProvider());
        self::assertNotNull($definition->getTableMetadataProvider());
    }

    public function testFileValidationInvalidData(): void
    {
        $definition = new InputMappingStagingDefinition(
            'foo',
            LocalFile::class,
            LocalTable::class,
            null,
            new NullProvider(),
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
            LocalFile::class,
            LocalTable::class,
            new NullProvider(),
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
            LocalFile::class,
            LocalTable::class,
            new NullProvider(),
            new NullProvider(),
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
            LocalFile::class,
            LocalTable::class,
            null,
            null,
            null,
            new NullProvider(),
        );
        $this->expectException(StagingException::class);
        $this->expectExceptionMessage('Undefined table data provider in "foo" staging.');
        $definition->validateFor(AbstractStagingDefinition::STAGING_TABLE);
    }

    public function testTableValidationInvalidMetadata(): void
    {
        $definition = new InputMappingStagingDefinition(
            'foo',
            LocalFile::class,
            LocalTable::class,
            null,
            null,
            new NullProvider(),
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
            LocalFile::class,
            LocalTable::class,
            null,
            null,
            new NullProvider(),
            new NullProvider(),
        );
        $definition->validateFor(AbstractStagingDefinition::STAGING_TABLE);
        self::assertTrue(true);
    }

    public function testTableValidationInvalidStagingType(): void
    {
        $definition = new InputMappingStagingDefinition(
            'foo',
            LocalFile::class,
            LocalTable::class,
            null,
            null,
            new NullProvider(),
            null,
        );
        $this->expectException(StagingException::class);
        $this->expectExceptionMessage('Unknown staging type: "invalid".');
        $definition->validateFor('invalid');
    }
}
