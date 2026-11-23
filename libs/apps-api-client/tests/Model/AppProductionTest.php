<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient\Tests\Model;

use Generator;
use Keboola\AppsApiClient\Model\AppProduction;
use PHPUnit\Framework\TestCase;
use TypeError;

class AppProductionTest extends TestCase
{
    private const DATA = [
        'desiredDeploymentId' => 'dep-desired',
        'actualDeploymentId' => 'dep-actual',
        'desiredConfigVersion' => '8',
        'actualConfigVersion' => '7',
        'desiredCreatedAt' => '2026-11-19T08:00:00+00:00',
        'activeRunId' => 'run-active',
    ];

    public function testFromResponseData(): void
    {
        $production = AppProduction::fromResponseData(self::DATA);

        self::assertSame('dep-desired', $production->desiredDeploymentId);
        self::assertSame('dep-actual', $production->actualDeploymentId);
        self::assertSame('8', $production->desiredConfigVersion);
        self::assertSame('7', $production->actualConfigVersion);
        self::assertSame('2026-11-19T08:00:00+00:00', $production->desiredCreatedAt);
        self::assertSame('run-active', $production->activeRunId);
    }

    /** A server that predates the field does not send it. */
    public function testAnAbsentActiveRunIdIsNull(): void
    {
        $data = self::DATA;
        unset($data['activeRunId']);

        self::assertNull(AppProduction::fromResponseData($data)->activeRunId);
    }

    /**
     * @dataProvider wrongTypeProvider
     */
    public function testFromResponseDataRejectsWrongType(array $data): void
    {
        $this->expectException(TypeError::class);

        AppProduction::fromResponseData($data);
    }

    public static function wrongTypeProvider(): Generator
    {
        foreach (array_keys(self::DATA) as $key) {
            yield 'non-string ' . $key => [array_merge(self::DATA, [$key => 42])];
        }
    }
}
