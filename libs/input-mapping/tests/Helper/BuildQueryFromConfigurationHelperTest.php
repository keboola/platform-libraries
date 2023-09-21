<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Helper;

use Keboola\InputMapping\Helper\BuildQueryFromConfigurationHelper;
use PHPUnit\Framework\TestCase;

class BuildQueryFromConfigurationHelperTest extends TestCase
{
    public function testGetTagsFromSourceTags(): void
    {
        self::assertEquals(
            ['componentId: keboola.ex-gmail', 'configurationId: 123'],
            BuildQueryFromConfigurationHelper::getTagsFromSourceTags([
                [
                    'name' => 'componentId: keboola.ex-gmail',
                ],
                [
                    'name' => 'configurationId: 123',
                ],
            ]),
        );
    }

    public function testGetSourceTagsFromTags(): void
    {
        self::assertEquals(
            [
                [
                    'name' => 'componentId: keboola.ex-gmail',
                ],
                [
                    'name' => 'configurationId: 123',
                ],
            ],
            BuildQueryFromConfigurationHelper::getSourceTagsFromTags([
                'componentId: keboola.ex-gmail',
                'configurationId: 123',
            ]),
        );
    }

    public function testBuildQueryForTags(): void
    {
        self::assertEquals(
            'tags:"componentId: keboola.ex-gmail" AND tags:"configurationId: 123"',
            BuildQueryFromConfigurationHelper::buildQueryForSourceTags([
                ['name' => 'componentId: keboola.ex-gmail', 'match' => 'include'],
                ['name' => 'configurationId: 123', 'match' => 'include'],
            ]),
        );
    }

    public function testBuildQueryForTagsExclude(): void
    {
        self::assertEquals(
            'tags:"componentId: keboola.ex-gmail" AND NOT tags:"configurationId: 123"',
            BuildQueryFromConfigurationHelper::buildQueryForSourceTags([
                ['name' => 'componentId: keboola.ex-gmail', 'match' => 'include'],
                ['name' => 'configurationId: 123', 'match' => 'exclude'],
            ]),
        );
    }

    public function testBuildQueryOnlyQuery(): void
    {
        self::assertEquals(
            'tag:123',
            BuildQueryFromConfigurationHelper::buildQuery([
                'query' => 'tag:123',
            ]),
        );
    }

    public function testBuildQueryOnlySourceTags(): void
    {
        self::assertEquals(
            'tags:"componentId: keboola.ex-gmail" AND tags:"configurationId: 123"',
            BuildQueryFromConfigurationHelper::buildQuery([
                'source' => [
                    'tags' => [
                        [
                            'name' => 'componentId: keboola.ex-gmail',
                            'match' => 'include',
                        ],
                        [
                            'name' => 'configurationId: 123',
                            'match' => 'include',
                        ],
                    ],
                ],
            ]),
        );
    }

    public function testBuildQuerySourceTagsAndQuery(): void
    {
        self::assertEquals(
            'tag:123 AND (tags:"componentId: keboola.ex-gmail" AND tags:"configurationId: 123")',
            BuildQueryFromConfigurationHelper::buildQuery([
                'query' => 'tag:123',
                'source' => [
                    'tags' => [
                        [
                            'name' => 'componentId: keboola.ex-gmail',
                            'match' => 'include',
                        ],
                        [
                            'name' => 'configurationId: 123',
                            'match' => 'include',
                        ],
                    ],
                ],
            ]),
        );
    }

    public function testChangedSinceQueryPortion(): void
    {
        self::assertEquals(
            sprintf('created:["%s" TO *]', date('c', strtotime('-5 days'))),
            BuildQueryFromConfigurationHelper::getChangedSinceQueryPortion('-5 days'),
        );
    }

    public function testBuildQueryChangedSinceWithQueryNoSourceTags(): void
    {
        self::assertStringContainsString(
            'tag:123',
            BuildQueryFromConfigurationHelper::buildQuery([
                'query' => 'tag:123',
                'changed_since' => '-5days',
            ]),
        );
    }

    public function testBuildQueryChangedSinceNoQuerySourceTags(): void
    {
        self::assertEquals(
            sprintf(
                '(tags:"componentId: keboola.ex-gmail" AND tags:"configurationId: 123") AND created:["%s" TO *]',
                date('c', strtotime('-5 days')),
            ),
            BuildQueryFromConfigurationHelper::buildQuery([
                'source' => [
                    'tags' => [
                        [
                            'name' => 'componentId: keboola.ex-gmail',
                            'match' => 'include',
                        ],
                        [
                            'name' => 'configurationId: 123',
                            'match' => 'include',
                        ],
                    ],
                ],
                'changed_since' => '-5days',
            ]),
        );
    }

    public function testBuildQueryChangedSinceExcludeSourceTags(): void
    {
        self::assertEquals(
            sprintf(
                // phpcs:ignore Generic.Files.LineLength
                '(tags:"componentId: keboola.ex-gmail" AND NOT tags:"runId: 12345" AND tags:"configurationId: 123") AND created:["%s" TO *]',
                date('c', strtotime('-5 days')),
            ),
            BuildQueryFromConfigurationHelper::buildQuery([
                'source' => [
                    'tags' => [
                        [
                            'name' => 'componentId: keboola.ex-gmail',
                            'match' => 'include',
                        ],
                        [
                            'name' => 'runId: 12345',
                            'match' => 'exclude',
                        ],
                        [
                            'name' => 'configurationId: 123',
                            'match' => 'include',
                        ],
                    ],
                ],
                'changed_since' => '-5days',
            ]),
        );
    }

    public function testBuildQueryChangedSinceAndOnlyExcludedSourceTags(): void
    {
        self::assertStringContainsString(
            sprintf(
                // phpcs:ignore Generic.Files.LineLength
                '(NOT tags:"componentId: keboola.ex-gmail" AND NOT tags:"runId: 12345" AND NOT tags:"configurationId: 123") AND created:["%s" TO *]',
                date('c', strtotime('-5 days')),
            ),
            BuildQueryFromConfigurationHelper::buildQuery([
                'source' => [
                    'tags' => [
                        [
                            'name' => 'componentId: keboola.ex-gmail',
                            'match' => 'exclude',
                        ],
                        [
                            'name' => 'runId: 12345',
                            'match' => 'exclude',
                        ],
                        [
                            'name' => 'configurationId: 123',
                            'match' => 'exclude',
                        ],
                    ],
                ],
                'changed_since' => '-5days',
            ]),
        );
    }
}
