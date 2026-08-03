<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Generator;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Helper\DescriptionHelper;
use PHPUnit\Framework\TestCase;

class DescriptionHelperTest extends TestCase
{
    /**
     * @dataProvider normalizeDescriptionProvider
     */
    public function testNormalizeDescription(mixed $description, ?string $expected): void
    {
        self::assertSame($expected, DescriptionHelper::normalizeDescription($description));
    }

    public static function normalizeDescriptionProvider(): Generator
    {
        yield 'text' => ['description' => 'some description', 'expected' => 'some description'];
        yield 'empty string is no description' => ['description' => '', 'expected' => null];
        yield 'whitespace is a description' => ['description' => ' ', 'expected' => ' '];
        yield 'null' => ['description' => null, 'expected' => null];
        yield 'integer is cast' => ['description' => 42, 'expected' => '42'];
        yield 'zero is cast' => ['description' => 0, 'expected' => '0'];
        yield 'false is no description' => ['description' => false, 'expected' => null];
        yield 'array' => ['description' => ['nope'], 'expected' => null];
    }

    public function testRemoveDescriptionFromMetadataMap(): void
    {
        $metadata = [
            'KBC.description' => 'some description',
            'KBC.datatype.type' => 'VARCHAR',
        ];

        self::assertSame(
            ['KBC.datatype.type' => 'VARCHAR'],
            DescriptionHelper::removeDescriptionFromMetadataMap($metadata, 'table_metadata'),
        );
    }

    public function testRemoveDescriptionFromMetadataMapKeepsMapWithoutDescription(): void
    {
        $metadata = ['KBC.datatype.type' => 'VARCHAR'];

        self::assertSame($metadata, DescriptionHelper::removeDescriptionFromMetadataMap($metadata, 'metadata'));
    }

    public function testGetDescriptionFromMetadataMap(): void
    {
        self::assertSame(
            'some description',
            DescriptionHelper::getDescriptionFromMetadataMap(
                ['KBC.description' => 'some description'],
                'table_metadata',
            ),
        );
    }

    /**
     * @dataProvider metadataMapWithoutDescriptionProvider
     */
    public function testGetDescriptionFromMetadataMapWithoutDescription(array $metadata): void
    {
        self::assertNull(DescriptionHelper::getDescriptionFromMetadataMap($metadata, 'table_metadata'));
    }

    public static function metadataMapWithoutDescriptionProvider(): Generator
    {
        yield 'empty map' => ['metadata' => []];
        yield 'other keys only' => ['metadata' => ['KBC.datatype.type' => 'VARCHAR']];
        yield 'empty description' => ['metadata' => ['KBC.description' => '']];
    }

    /**
     * The node is a variableNode in the configuration, so a non-object value reaches the code. It is a
     * configuration error and must be reported instead of silently dropping the metadata.
     *
     * @dataProvider notAMetadataMapProvider
     */
    public function testMetadataMapMustBeAnObject(mixed $metadata, string $expectedType): void
    {
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage(sprintf(
            'Configuration node "table_metadata" must be an object, "%s" given.',
            $expectedType,
        ));

        DescriptionHelper::removeDescriptionFromMetadataMap($metadata, 'table_metadata');
    }

    /**
     * @dataProvider notAMetadataMapProvider
     */
    public function testMetadataMapMustBeAnObjectWhenReadingDescription(mixed $metadata, string $expectedType): void
    {
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage(sprintf(
            'Configuration node "table_metadata" must be an object, "%s" given.',
            $expectedType,
        ));

        DescriptionHelper::getDescriptionFromMetadataMap($metadata, 'table_metadata');
    }

    public static function notAMetadataMapProvider(): Generator
    {
        yield 'string' => ['metadata' => 'some description', 'expectedType' => 'string'];
        yield 'integer' => ['metadata' => 42, 'expectedType' => 'int'];
        yield 'boolean' => ['metadata' => true, 'expectedType' => 'bool'];
    }

    public function testRemoveDescriptionFromMetadataList(): void
    {
        $metadata = [
            ['key' => 'KBC.datatype.type', 'value' => 'VARCHAR'],
            ['key' => 'KBC.description', 'value' => 'some description'],
            ['key' => 'KBC.datatype.nullable', 'value' => true],
        ];

        self::assertSame(
            [
                ['key' => 'KBC.datatype.type', 'value' => 'VARCHAR'],
                ['key' => 'KBC.datatype.nullable', 'value' => true],
            ],
            DescriptionHelper::removeDescriptionFromMetadataList($metadata),
        );
    }

    public function testRemoveDescriptionFromMetadataListRemovesEveryDescriptionItem(): void
    {
        $metadata = [
            ['key' => 'KBC.description', 'value' => 'first'],
            ['key' => 'KBC.description', 'value' => 'second'],
        ];

        self::assertSame([], DescriptionHelper::removeDescriptionFromMetadataList($metadata));
    }

    public function testGetDescriptionFromMetadataList(): void
    {
        $metadata = [
            ['key' => 'KBC.datatype.type', 'value' => 'VARCHAR'],
            ['key' => 'KBC.description', 'value' => 'some description'],
        ];

        self::assertSame('some description', DescriptionHelper::getDescriptionFromMetadataList($metadata));
    }

    /**
     * The schema does not prevent a list from carrying more than one description; the first item decides, so
     * that the table and the column level agree on the same input.
     */
    public function testGetDescriptionFromMetadataListReadsTheFirstItem(): void
    {
        $metadata = [
            ['key' => 'KBC.description', 'value' => 'first'],
            ['key' => 'KBC.description', 'value' => 'second'],
        ];

        self::assertSame('first', DescriptionHelper::getDescriptionFromMetadataList($metadata));
    }

    /**
     * @dataProvider metadataListWithoutDescriptionProvider
     */
    public function testGetDescriptionFromMetadataListWithoutDescription(array $metadata): void
    {
        self::assertNull(DescriptionHelper::getDescriptionFromMetadataList($metadata));
    }

    public static function metadataListWithoutDescriptionProvider(): Generator
    {
        yield 'empty list' => ['metadata' => []];
        yield 'other keys only' => ['metadata' => [['key' => 'KBC.datatype.type', 'value' => 'VARCHAR']]];
        yield 'empty description' => ['metadata' => [['key' => 'KBC.description', 'value' => '']]];
        yield 'first item empty' => ['metadata' => [
            ['key' => 'KBC.description', 'value' => ''],
            ['key' => 'KBC.description', 'value' => 'second'],
        ]];
    }
}
