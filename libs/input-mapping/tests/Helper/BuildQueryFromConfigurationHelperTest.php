<?php

namespace Keboola\InputMapping\Tests\Helper;

use Keboola\InputMapping\Helper\BuildQueryFromConfigurationHelper;
use PHPUnit\Framework\TestCase;

class BuildQueryFromConfigurationHelperTest extends TestCase
{
    public function testGetTagsFromSourceTags()
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
            ])
        );
    }

    public function testGetSourceTagsFromTags()
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
            ])
        );
    }

    public function testBuildQueryForTags()
    {
        self::assertEquals(
            'tags:"componentId: keboola.ex-gmail" AND tags:"configurationId: 123"',
            BuildQueryFromConfigurationHelper::buildQueryForSourceTags([
                ['name' => 'componentId: keboola.ex-gmail', 'match' => 'include'],
                ['name' => 'configurationId: 123', 'match' => 'include'],
            ])
        );
    }

    public function testBuildQueryForTagsExclude()
    {
        self::assertEquals(
            'tags:"componentId: keboola.ex-gmail" AND NOT tags:"configurationId: 123"',
            BuildQueryFromConfigurationHelper::buildQueryForSourceTags([
                ['name' => 'componentId: keboola.ex-gmail', 'match' => 'include'],
                ['name' => 'configurationId: 123', 'match' => 'exclude'],
            ])
        );
    }

    public function testBuildQueryOnlyQuery()
    {
        self::assertEquals(
            'tag:123',
            BuildQueryFromConfigurationHelper::buildQuery([
                'query' => 'tag:123',
            ])
        );
    }

    public function testBuildQueryOnlySourceTags()
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
            ])
        );
    }

    public function testBuildQuerySourceTagsAndQuery()
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
            ])
        );
    }

    public function testChangedSinceQueryPortion()
    {
        $dateTime = date('Y-m-d', strtotime('-5 days'));
        self::assertContains(
            sprintf('created:>%s', $dateTime),
            BuildQueryFromConfigurationHelper::getChangedSinceQueryPortion('-5 days')
        );
    }

    public function testBuildQueryChangedSinceWithQueryNoSourceTags()
    {
        $dateTime = date('Y-m-d', strtotime('-5 days'));
        self::assertContains(
            sprintf('tag:123 AND created:>%s', $dateTime),
            BuildQueryFromConfigurationHelper::buildQuery([
                'query' => 'tag:123',
                'changed_since' => '-5days',
            ])
        );
    }

    public function testBuildQueryChangedSinceNoQuerySourceTags()
    {
        $dateTime = date('Y-m-d', strtotime('-5 days'));
        self::assertContains(
            sprintf(
                'tags:"componentId: keboola.ex-gmail" AND tags:"configurationId: 123" AND created:>%s',
                $dateTime
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
            ])
        );
    }
}
