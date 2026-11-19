<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient\Tests\Model;

use Generator;
use Keboola\AppsApiClient\Model\AppProduction;
use PHPUnit\Framework\TestCase;
use Webmozart\Assert\InvalidArgumentException;

class AppProductionTest extends TestCase
{
    private const DATA = [
        'desiredDeploymentId' => 'dep-desired',
        'actualDeploymentId' => 'dep-actual',
        'desiredConfigVersion' => '8',
        'actualConfigVersion' => '7',
        'desiredCreatedAt' => '2026-11-19T08:00:00+00:00',
    ];

    public function testFromResponseData(): void
    {
        $production = AppProduction::fromResponseData(self::DATA);

        self::assertSame('dep-desired', $production->desiredDeploymentId);
        self::assertSame('dep-actual', $production->actualDeploymentId);
        self::assertSame('8', $production->desiredConfigVersion);
        self::assertSame('7', $production->actualConfigVersion);
        self::assertSame('2026-11-19T08:00:00+00:00', $production->desiredCreatedAt);
    }

    /**
     * @dataProvider invalidDataProvider
     */
    public function testFromResponseDataRejectsInvalidData(array $data): void
    {
        $this->expectException(InvalidArgumentException::class);

        AppProduction::fromResponseData($data);
    }

    public static function invalidDataProvider(): Generator
    {
        foreach (array_keys(self::DATA) as $key) {
            $missing = self::DATA;
            unset($missing[$key]);
            yield 'missing ' . $key => [$missing];
            yield 'non-string ' . $key => [array_merge(self::DATA, [$key => 42])];
        }
    }
}
