<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Configuration;

use Generator;
use Keboola\InputMapping\Configuration\File;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class FileConfigurationTest extends TestCase
{
    public function testConfiguration(): void
    {
        $config = [
            'tags' => ['tag1', 'tag2'],
            'query' => 'esquery',
            'processed_tags' => ['tag3'],
            'limit' => 1000,
            'overwrite' => false,
        ];
        $expectedResponse = $config;
        $processedConfiguration = (new File())->parse(['config' => $config]);
        self::assertSame($expectedResponse, $processedConfiguration);
    }

    public function testEmptyTagsRemoved(): void
    {
        $config = [
            'tags' => [],
            'query' => 'esquery',
            'processed_tags' => ['tag3'],
            'limit' => 1000,
            'overwrite' => true,
        ];
        $expectedResponse = $config;
        unset($expectedResponse['tags']);
        $processedConfiguration = (new File())->parse([
            'config' => $config,
        ]);
        self::assertSame($expectedResponse, $processedConfiguration);
    }

    public function testEmptyProcessedTagsRemoved(): void
    {
        $config = [
            'tags' => ['tag3'],
            'query' => 'esquery',
            'processed_tags' => [],
            'limit' => 1000,
            'overwrite' => true,
        ];
        $expectedResponse = $config;
        unset($expectedResponse['processed_tags']);
        $processedConfiguration = (new File())->parse([
            'config' => $config,
        ]);
        self::assertSame($expectedResponse, $processedConfiguration);
    }

    public function testEmptyQueryRemoved(): void
    {
        $config = [
            'tags' => ['tag1'],
            'query' => '',
            'processed_tags' => ['tag3'],
            'limit' => 1000,
            'overwrite' => true,
        ];
        $expectedResponse = $config;
        unset($expectedResponse['query']);
        $processedConfiguration = (new File())->parse([
            'config' => $config,
        ]);
        self::assertSame($expectedResponse, $processedConfiguration);
    }

    public function testConfigurationWithSourceTags(): void
    {
        $config = [
            'query' => 'esquery',
            'processed_tags' => ['tag3'],
            'limit' => 1000,
            'source' => [
                'tags' => [
                    [
                        'name' => 'tag1',
                        'match' => 'include',
                    ],
                    [
                        'name' => 'tag2',
                        'match' => 'include',
                    ],
                ],
            ],
            'overwrite' => true,
        ];
        $expectedResponse = $config;
        $processedConfiguration = (new File())->parse(['config' => $config]);
        self::assertSame($expectedResponse, $processedConfiguration);
    }

    public function testEmptyConfiguration(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'Invalid configuration for path "file": ' .
            'At least one of "tags", "source.tags", "query" or "file_ids" parameters must be defined.',
        );
        (new File())->parse(['config' => []]);
    }

    public function testConfigurationWithTagsAndSourceTags(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'Invalid configuration for path "file": Both "tags" and "source.tags" cannot be defined.',
        );
        (new File())->parse(['config' => [
            'tags' => ['tag1'],
            'source' => [
                'tags' => [
                    [
                        'name' => 'tag1',
                    ],
                    [
                        'name' => 'tag2',
                    ],
                ],
            ],
        ]]);
    }

    public function testValidAdaptiveInputConfigurationWithTags(): void
    {
        $config = [
            'tags' => ['tag'],
            'changed_since' => 'adaptive',
            'overwrite' => true,
        ];
        $expectedResponse = $config;
        $processedConfiguration = (new File())->parse(['config' => $config]);
        self::assertSame($expectedResponse, $processedConfiguration);
    }

    public function testOverwriteDefault(): void
    {
        $config = [
            'tags' => ['tag'],
            'changed_since' => 'adaptive',
        ];
        $expectedResponse = $config;
        $expectedResponse['overwrite'] = true;
        $processedConfiguration = (new File())->parse(['config' => $config]);
        self::assertSame($expectedResponse, $processedConfiguration);
    }

    public function testValidAdaptiveInputConfigurationWithSourceTags(): void
    {
        $config = [
            'source' => [
                'tags' => [
                    [
                        'name' => 'tag',
                        'match' => 'include',
                    ],
                ],
            ],
            'changed_since' => 'adaptive',
            'overwrite' => true,
        ];
        $expectedResponse = $config;
        $processedConfiguration = (new File())->parse(['config' => $config]);
        self::assertSame($expectedResponse, $processedConfiguration);
    }

    public function testConfigurationWithQueryAndChangedSince(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('The changed_since parameter is not supported for query configurations');
        (new File())->parse(['config' => [
            'query' => 'some query',
            'changed_since' => 'adaptive',
        ]]);
    }

    public function testConfigurationWithInvalidChangedSince(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('The value provided for changed_since could not be converted to a timestamp');
        (new File())->parse(['config' => [
            'tags' => ['tag123'],
            'changed_since' => '-1 light year',
        ]]);
    }

    public function testEmptyFileIdsRemoved(): void
    {
        $config = [
            'tags' => ['tag1'],
            'query' => 'aaa',
            'file_ids' => [],
            'processed_tags' => ['tag3'],
            'limit' => 1000,
            'overwrite' => true,
        ];
        $expectedResponse = $config;
        unset($expectedResponse['file_ids']);
        $processedConfiguration = (new File())->parse([
            'config' => $config,
        ]);
        self::assertSame($expectedResponse, $processedConfiguration);
    }

    public function testConfigurationWithFileIds(): void
    {
        $config = [
            'file_ids' => ['123', 456],
            'overwrite' => false,
            'processed_tags' => ['downloaded'],
        ];
        $expectedResponse = $config;
        $processedConfiguration = (new File())->parse(['config' => $config]);
        self::assertSame($expectedResponse, $processedConfiguration);
    }

    public function fileIdsCannotBeCombinedWithOtherFiltersProvider(): Generator
    {
        yield [
            ['tags' => ['tag1']],
        ];

        yield [
            ['query' => 'abc'],
        ];

        yield [
            ['limit' => 1],
        ];

        yield [
            ['changed_since' => '-7 days'],
            ];

        yield [
            [
                'source' => [
                    'tags' => [
                        [
                            'name' => 'tag1',
                            'match' => 'include',
                        ],
                    ],
                ],
            ],
        ];
    }

    /**
     * @dataProvider fileIdsCannotBeCombinedWithOtherFiltersProvider
     */
    public function testFileIdsCannotBeCombinedWithOtherFilters(array $filters): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'The file_ids filter can be combined only with overwrite flag and processed_tags',
        );
        (new File())->parse(['config' => array_merge(['file_ids' => [123]], $filters)]);
    }
}
