<?php

namespace Keboola\InputMapping\Tests\Staging;

use Keboola\InputMapping\Exception\StagingException;
use Keboola\InputMapping\Staging\Definition;
use Keboola\InputMapping\Staging\NullProvider;
use PHPUnit\Framework\TestCase;

class DefinitionTest extends TestCase
{
    public function testAccessors()
    {
        $definition = new Definition('foo', 'bar', 'kochba');
        self::assertSame('foo', $definition->getName());
        self::assertSame('bar', $definition->getFileStagingClass());
        self::assertSame('kochba', $definition->getTableStagingClass());
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

    public function testFileValidationInvalidName()
    {
        $definition = new Definition('foo', '', 'kochba', new NullProvider(), new NullProvider(), null, null);
        self::expectException(StagingException::class);
        self::expectExceptionMessage('Undefined file class in "foo" staging.');
        $definition->validateFor(Definition::STAGING_FILE);
    }

    public function testFileValidationInvalidData()
    {
        $definition = new Definition('foo', 'bar', 'kochba', null, new NullProvider(), null, null);
        self::expectException(StagingException::class);
        self::expectExceptionMessage('Undefined file data provider in "foo" staging.');
        $definition->validateFor(Definition::STAGING_FILE);
    }

    public function testFileValidationInvalidMetadata()
    {
        $definition = new Definition('foo', 'bar', 'kochba', new NullProvider(), null, null, null);
        self::expectException(StagingException::class);
        self::expectExceptionMessage('Undefined file metadata provider in "foo" staging.');
        $definition->validateFor(Definition::STAGING_FILE);
    }

    public function testFileValidation()
    {
        $definition = new Definition('foo', 'bar', 'kochba', new NullProvider(), new NullProvider(), null, null);
        $definition->validateFor(Definition::STAGING_FILE);
        self::assertTrue(true);
    }

    public function testTableValidationInvalidName()
    {
        $definition = new Definition('foo', 'bar', '', null, null, new NullProvider(), new NullProvider());
        self::expectException(StagingException::class);
        self::expectExceptionMessage('Undefined table class in "foo" staging.');
        $definition->validateFor(Definition::STAGING_TABLE);
    }

    public function testTableValidationInvalidData()
    {
        $definition = new Definition('foo', 'bar', 'kochba', null, null, null, new NullProvider());
        self::expectException(StagingException::class);
        self::expectExceptionMessage('Undefined table data provider in "foo" staging.');
        $definition->validateFor(Definition::STAGING_TABLE);
    }

    public function testTableValidationInvalidMetadata()
    {
        $definition = new Definition('foo', 'bar', 'kochba', null, null, new NullProvider(), null);
        self::expectException(StagingException::class);
        self::expectExceptionMessage('Undefined table metadata provider in "foo" staging.');
        $definition->validateFor(Definition::STAGING_TABLE);
    }

    public function testTableValidation()
    {
        $definition = new Definition('foo', 'bar', 'kochba', null, null, new NullProvider(), new NullProvider());
        $definition->validateFor(Definition::STAGING_TABLE);
        self::assertTrue(true);
    }

    public function testTableValidationInvalidStagingType()
    {
        $definition = new Definition('foo', 'bar', 'kochba', null, null, new NullProvider(), null);
        self::expectException(StagingException::class);
        self::expectExceptionMessage('Unknown staging type: "invalid".');
        $definition->validateFor('invalid');
    }
}
