<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\File\Options;

use Generator;
use Keboola\InputMapping\File\Options\RewrittenInputFileOptions;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\StorageApi\Options\ListFilesOptions;
use PHPUnit\Framework\TestCase;

class RewrittenInputFileOptionsTest extends TestCase
{
    public function testGetters(): void
    {
        $options = new RewrittenInputFileOptions(
            ['tags' => ['foo']],
            false,
            '1234',
            ['tags' => ['bar']],
            123,
        );

        self::assertSame(['foo'], $options->getTags());
        self::assertFalse($options->isDevBranch());
        self::assertSame('1234', $options->getRunId());
        self::assertSame(
            [
                'tags' => ['foo'],
                'overwrite' => true,
            ],
            $options->getDefinition(),
        );
        self::assertSame(
            [['name' => 'bar']],
            $options->getFileConfigurationIdentifier(),
        );
        self::assertSame(123, $options->getSourceBranchId());
    }

    /** @dataProvider provideFileListOptionsValues */
    public function testGetStorageApiFileListOptions(
        array $rewrittenFileOptions,
        bool $isDevBranch,
        string $runId,
        InputFileStateList $fileStates,
        ListFilesOptions $expectedOptions,
        ?array $originalFileOptions,
    ): void {
        if ($originalFileOptions === null) {
            $originalFileOptions = $rewrittenFileOptions;
        }
        $inputFileOptions = new RewrittenInputFileOptions(
            $rewrittenFileOptions,
            $isDevBranch,
            $runId,
            $originalFileOptions,
            123,
        );
        $options = $inputFileOptions->getStorageApiFileListOptions($fileStates);
        self::assertEquals($expectedOptions, $options);
    }

    public function provideFileListOptionsValues(): Generator
    {
        yield 'tags' => [
            'rewrittenFileOptions' => ['tags' => ['foo']],
            'isDevBranch' => false,
            'runId' => '1234',
            'filesStates' => new InputFileStateList([]),
            'expectedOptions' => (new ListFilesOptions())->setTags(['foo'])->setLimit(100),
            'originalFileOptions' => null,
        ];
        yield 'tags + limit' => [
            'rewrittenFileOptions' => ['tags' => ['foo'], 'limit' => 50],
            'isDevBranch' => false,
            'runId' => '1234',
            'filesStates' => new InputFileStateList([]),
            'expectedOptions' => (new ListFilesOptions())->setTags(['foo'])->setLimit(50),
            'originalFileOptions' => null,
        ];
        yield 'source tags include' => [
            'rewrittenFileOptions' => ['source' =>
                ['tags' => [['name' => 'foo', 'match' => 'include']]],
                'limit' => 50,
            ],
            'isDevBranch' => false,
            'runId' => '1234',
            'filesStates' => new InputFileStateList([]),
            'expectedOptions' => (new ListFilesOptions())->setQuery('tags:"foo"')->setLimit(50),
            'originalFileOptions' => null,
        ];
        yield 'source tags exclude' => [
            'rewrittenFileOptions' => ['source' => ['tags' => [['name' => 'foo', 'match' => 'exclude']]]],
            'isDevBranch' => false,
            'runId' => '1234',
            'filesStates' => new InputFileStateList([]),
            'expectedOptions' => (new ListFilesOptions())->setQuery('NOT tags:"foo"')->setLimit(100),
            'originalFileOptions' => null,
        ];
        yield 'query' => [
            'rewrittenFileOptions' => ['query' => 'foo bar'],
            'isDevBranch' => false,
            'runId' => '1234',
            'filesStates' => new InputFileStateList([]),
            'expectedOptions' => (new ListFilesOptions())->setQuery('foo bar'),
            'originalFileOptions' => null,
        ];
        yield 'changed_since + tags' => [
            'rewrittenFileOptions' => ['tags' => ['foo'], 'changed_since' => '2023-07-18T12:00:00+00:00'],
            'isDevBranch' => false,
            'runId' => '1234',
            'filesStates' => new InputFileStateList([]),
            'expectedOptions' => (new ListFilesOptions())->setTags(['foo'])
                ->setQuery('created:["2023-07-18T12:00:00+00:00" TO *]'),
            'originalFileOptions' => null,
        ];
        yield 'changed_since adaptive + tags + no state' => [
            'rewrittenFileOptions' => ['tags' => ['foo'], 'changed_since' => 'adaptive'],
            'isDevBranch' => false,
            'runId' => '1234',
            'filesStates' => new InputFileStateList([]),
            'expectedOptions' => (new ListFilesOptions())->setTags(['foo']),
            'originalFileOptions' => null,
        ];
        yield 'changed_since adaptive + tags + some state' => [
            'rewrittenFileOptions' => ['tags' => ['foo'], 'changed_since' => 'adaptive'],
            'isDevBranch' => false,
            'runId' => '1234',
            'filesStates' => new InputFileStateList([
                [
                    'tags' => [['name' => 'foo']],
                    'lastImportId' => '1234567',
                ],
            ]),
            'expectedOptions' => (new ListFilesOptions())->setTags(['foo'])->setSinceId('1234567'),
            'originalFileOptions' => null,
        ];
        yield 'changed_since adaptive + tags + invalid state' => [
            'rewrittenFileOptions' => ['tags' => ['foo'], 'changed_since' => 'adaptive'],
            'isDevBranch' => false,
            'runId' => '1234',
            'filesStates' => new InputFileStateList([
                [
                    'tags' => [['name' => 'bar']],
                    'lastImportId' => '1234567',
                ],
            ]),
            'expectedOptions' => (new ListFilesOptions())->setTags(['foo']),
            'originalFileOptions' => null,
        ];
        yield 'changed_since adaptive + tags + state + branch' => [
            'rewrittenFileOptions' => ['tags' => ['foo'], 'changed_since' => 'adaptive'],
            'isDevBranch' => true,
            'runId' => '1234',
            'filesStates' => new InputFileStateList([
                [
                    'tags' => [['name' => 'foo']],
                    'lastImportId' => '1234567',
                ],
            ]),
            'expectedOptions' => (new ListFilesOptions())->setTags(['foo'])->setSinceId('1234567'),
            'originalFileOptions' => null,
        ];
        yield 'changed_since adaptive + tags + state + branch + rewritten tags' => [
            'rewrittenFileOptions' => ['tags' => ['1234-foo'], 'changed_since' => 'adaptive'],
            'isDevBranch' => true,
            'runId' => '1234',
            'filesStates' => new InputFileStateList([
                [
                    /* even that the tag is 1234-foo, the state from foo is still used, frankly I have no idea why
                        the state is not rewritten as well, but that it how it works now */
                    'tags' => [['name' => 'foo']],
                    'lastImportId' => '1234567',
                ],
            ]),
            'expectedOptions' => (new ListFilesOptions())->setTags(['1234-foo'])->setSinceId('1234567'),
            'originalFileOptions' => ['tags' => ['foo']],
        ];
        yield 'single file in file_ids' => [
            'rewrittenFileOptions' => ['file_ids' => [123]],
            'isDevBranch' => false,
            'runId' => '1234',
            'filesStates' => new InputFileStateList([]),
            'expectedOptions' => (new ListFilesOptions())->setQuery('id:(123)')->setLimit(100),
            'originalFileOptions' => null,
        ];
        yield 'single file in file_ids + branch' => [
            'rewrittenFileOptions' => ['file_ids' => [123]],
            'isDevBranch' => true,
            'runId' => '1234',
            'filesStates' => new InputFileStateList([]),
            'expectedOptions' => (new ListFilesOptions())->setQuery('id:(123)')->setLimit(100),
            'originalFileOptions' => null,
        ];
        yield 'multiple files in file_ids' => [
            'rewrittenFileOptions' => ['file_ids' => [123, '456']],
            'isDevBranch' => false,
            'runId' => '1234',
            'filesStates' => new InputFileStateList([]),
            'expectedOptions' => (new ListFilesOptions())->setQuery('id:(123 OR 456)')->setLimit(100),
            'originalFileOptions' => null,
        ];
    }
}
