<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Configuration;

use Keboola\OutputMapping\Configuration\Table;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class TableTest extends TestCase
{
    public function testBasicConfiguration(): void
    {
        $config = [
            'source' => 'data.csv',
            'destination' => 'in.c-main.test',
        ];

        $expectedArray = [
            'source' => 'data.csv',
            'destination' => 'in.c-main.test',
            'primary_key' => [],
            'distribution_key' => [],
            'columns' => [],
            'incremental' => false,
            'delete_where_values' => [],
            'delete_where_operator' => 'eq',
            'delimiter' => ',',
            'enclosure' => '"',
            'metadata' => [],
            'column_metadata' => [],
            'write_always' => false,
            'import_method' => 'auto',
            'tags' => [],
            'schema' => [],
        ];

        $processedConfiguration = (new Table())->parse(['config' => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }

    public function testComplexConfiguration(): void
    {
        $config = [
            'source' => 'test',
            'destination' => 'in.c-main.test',
            'incremental' => true,
            'primary_key' => ['Id', 'Name'],
            'distribution_key' => [],
            'columns' => ['Id', 'Name', 'status'],
            'delete_where_column' => 'status',
            'delete_where_values' => ['val1', 'val2'],
            'delete_where_operator' => 'ne',
            'delimiter' => '\t',
            'enclosure' => '\'',
            'metadata' => [],
            'column_metadata' => [],
            'write_always' => false,
            'import_method' => 'auto',
            'tags' => [],
            'schema' => [],
        ];

        $expectedArray = $config;

        $processedConfiguration = (new Table())->parse(['config' => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }

    public function testInvalidWhereOperator(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'delete_where_operator' => 'abc',
        ];

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'Invalid configuration for path "table.delete_where_operator": ' .
            'Invalid operator in delete_where_operator "abc"',
        );
        (new Table())->parse(['config' => $config]);
    }

    public function testEmptyConfiguration(): void
    {
        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessage('The child config "source" under "table" must be configured.');

        (new Table())->parse(['config' => []]);
    }

    public function testPrimaryKeyEmptyString(): void
    {
        $config = [
            'source' => 'test',
            'destination' => 'in.c-main.test',
            'primary_key' => [''],
        ];

        $expectedArray = [
            'source' => 'test',
            'destination' => 'in.c-main.test',
            'primary_key' => [''],
            'distribution_key' => [],
            'columns' => [],
            'incremental' => false,
            'delete_where_values' => [],
            'delete_where_operator' => 'eq',
            'delimiter' => ',',
            'enclosure' => '"',
            'metadata' => [],
            'column_metadata' => [],
            'write_always' => false,
            'import_method' => 'auto',
            'tags' => [],
            'schema' => [],
        ];
        $processedConfiguration = (new Table())->parse(['config' => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }

    public function testEmptyDeleteWhereOp(): void
    {
        $config = [
            'source' => 'data.csv',
            'destination' => 'in.c-main.test',
            'delete_where_operator' => '',
        ];

        $expectedArray = [
            'source' => 'data.csv',
            'destination' => 'in.c-main.test',
            'primary_key' => [],
            'distribution_key' => [],
            'columns' => [],
            'incremental' => false,
            'delete_where_values' => [],
            'delete_where_operator' => 'eq',
            'delimiter' => ',',
            'enclosure' => '"',
            'metadata' => [],
            'column_metadata' => [],
            'write_always' => false,
            'import_method' => 'auto',
            'tags' => [],
            'schema' => [],
        ];

        $processedConfiguration = (new Table())->parse(['config' => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }

    public function testSourceNotRequiredWithDirectGrantUnloadStrategy(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'unload_strategy' => 'direct-grant',
        ];

        $expectedArray = [
            'destination' => 'in.c-main.test',
            'unload_strategy' => 'direct-grant',
            'primary_key' => [],
            'distribution_key' => [],
            'columns' => [],
            'incremental' => false,
            'delete_where_values' => [],
            'delete_where_operator' => 'eq',
            'delimiter' => ',',
            'enclosure' => '"',
            'metadata' => [],
            'column_metadata' => [],
            'write_always' => false,
            'import_method' => 'auto',
            'tags' => [],
            'schema' => [],
        ];

        $processedConfiguration = (new Table())->parse(['config' => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }

    public function testSourceRequiredWithOtherUnloadStrategy(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'unload_strategy' => 'other-strategy',
        ];

        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessage('The child config "source" under "table" must be configured.');

        (new Table())->parse(['config' => $config]);
    }

    public function testPolicyTagsAreAccepted(): void
    {
        $policyTags = [
            'email' => 'projects/acme-gov/locations/eu/taxonomies/123/policyTags/456',
            '123' => 'projects/acme-gov/locations/eu/taxonomies/123/policyTags/789',
        ];
        $config = [
            'source' => 'data.csv',
            'destination' => 'in.c-main.test',
            'policy_tags' => $policyTags,
        ];

        $processedConfiguration = (new Table())->parse(['config' => $config]);
        self::assertSame($policyTags, $processedConfiguration['policy_tags']);

        $processedConfiguration = (new Table\Configuration())->parse(['config' => $config]);
        self::assertSame($policyTags, $processedConfiguration['policy_tags']);
    }

    public function testEmptyPolicyTagsAreAccepted(): void
    {
        $processedConfiguration = (new Table())->parse(['config' => [
            'source' => 'data.csv',
            'destination' => 'in.c-main.test',
            'policy_tags' => [],
        ]]);
        self::assertSame([], $processedConfiguration['policy_tags']);
    }

    public function testPolicyTagsHaveNoDefault(): void
    {
        // Older readers reject the key, so it must never be added to a configuration which does not set it.
        $config = [
            'source' => 'data.csv',
            'destination' => 'in.c-main.test',
        ];

        self::assertArrayNotHasKey('policy_tags', (new Table())->parse(['config' => $config]));
        self::assertArrayNotHasKey('policy_tags', (new Table\Configuration())->parse(['config' => $config]));
        unset($config['source']);
        self::assertArrayNotHasKey('policy_tags', (new Table\Manifest())->parse(['config' => $config]));
    }

    /** @dataProvider provideInvalidPolicyTags */
    public function testInvalidPolicyTags(mixed $policyTags): void
    {
        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessage(
            'The "policy_tags" option must be a map of column names to non-empty tag names',
        );

        (new Table())->parse(['config' => [
            'source' => 'data.csv',
            'destination' => 'in.c-main.test',
            'policy_tags' => $policyTags,
        ]]);
    }

    public static function provideInvalidPolicyTags(): iterable
    {
        yield 'string' => ['projects/p/locations/eu/taxonomies/1/policyTags/2'];
        yield 'list' => [['projects/p/locations/eu/taxonomies/1/policyTags/2']];
        yield 'empty tag' => [['email' => '']];
        yield 'non-string tag' => [['email' => 123]];
        yield 'nested tag' => [['email' => ['name' => 'projects/p/locations/eu/taxonomies/1/policyTags/2']]];
        yield 'empty column' => [['' => 'projects/p/locations/eu/taxonomies/1/policyTags/2']];
        yield 'null' => [null];
    }
}
