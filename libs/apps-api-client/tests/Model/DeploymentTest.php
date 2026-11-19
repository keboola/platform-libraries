<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient\Tests\Model;

use Generator;
use Keboola\AppsApiClient\Model\Deployment;
use Keboola\AppsApiClient\Tests\AppTestData;
use PHPUnit\Framework\TestCase;
use Webmozart\Assert\InvalidArgumentException;

class DeploymentTest extends TestCase
{
    public function testFromResponseData(): void
    {
        $deployment = Deployment::fromResponseData(AppTestData::deployment());

        self::assertSame(AppTestData::DEPLOYMENT_ID, $deployment->id);
        self::assertSame('7', $deployment->configVersion);
        self::assertNull($deployment->gitCommitSha);
        self::assertSame('small', $deployment->runtimeSize);
        self::assertSame(Deployment::BUILD_STATE_READY, $deployment->buildState);
        self::assertSame(Deployment::RUN_STATE_NEVER_RUN, $deployment->runState);
        self::assertFalse($deployment->archived);
        self::assertNull($deployment->archivedAt);
        self::assertSame('2026-11-19T08:00:00+00:00', $deployment->createdAt);
    }

    public function testFromResponseDataOfArchivedDeployment(): void
    {
        $deployment = Deployment::fromResponseData(AppTestData::deployment([
            'gitCommitSha' => 'abc123',
            'runtimeSize' => null,
            'runState' => 'stopped',
            'archived' => true,
            'archivedAt' => '2026-11-19T09:00:00+00:00',
        ]));

        self::assertSame('abc123', $deployment->gitCommitSha);
        self::assertNull($deployment->runtimeSize);
        self::assertSame(Deployment::RUN_STATE_STOPPED, $deployment->runState);
        self::assertTrue($deployment->archived);
        self::assertSame('2026-11-19T09:00:00+00:00', $deployment->archivedAt);
    }

    public function testFromResponseDataRejectsMissingRunState(): void
    {
        $data = AppTestData::deployment();
        unset($data['runState']);

        $this->expectException(InvalidArgumentException::class);

        Deployment::fromResponseData($data);
    }

    /**
     * @dataProvider wrongTypeProvider
     */
    public function testFromResponseDataRejectsWrongType(array $data): void
    {
        $this->expectException(InvalidArgumentException::class);

        Deployment::fromResponseData($data);
    }

    public static function wrongTypeProvider(): Generator
    {
        yield 'id of wrong type' => [AppTestData::deployment(['id' => 1])];
        yield 'runState of wrong type' => [AppTestData::deployment(['runState' => null])];
        yield 'archived of wrong type' => [AppTestData::deployment(['archived' => 0])];
        yield 'configVersion of wrong type' => [AppTestData::deployment(['configVersion' => 7])];
        yield 'archivedAt of wrong type' => [AppTestData::deployment(['archivedAt' => 1])];
    }
}
