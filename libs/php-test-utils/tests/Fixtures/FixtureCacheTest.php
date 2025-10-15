<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Tests\Fixtures;

use Keboola\PhpTestUtils\Fixtures\BackendType;
use Keboola\PhpTestUtils\Fixtures\FixtureCache;
use PHPUnit\Framework\TestCase;

class FixtureCacheTest extends TestCase
{
    public function testDestructCleansUpReusableAndNonReusableFixtures(): void
    {
        $reusable = new DummyFixture();
        $nonReusable = new DummyFixture();

        // add to cache
        FixtureCache::add(
            $reusable,
            'dummy_reusable',
            BackendType::SNOWFLAKE,
            true,
            'testMethod',
            'dataSet',
        );
        FixtureCache::add(
            $nonReusable,
            'dummy_non_reusable',
            BackendType::BIGQUERY,
            false,
            'testMethod',
            'dataSet',
        );

        // act - simulate shutdown
        FixtureCache::destruct();

        // assert both fixtures had cleanup called
        self::assertGreaterThanOrEqual(
            2,
            DummyFixture::$cleanUpCalls,
            'Both reusable and non-reusable fixtures should be cleaned up.',
        );
    }
}
