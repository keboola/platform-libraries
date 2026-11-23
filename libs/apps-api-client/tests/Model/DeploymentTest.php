<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient\Tests\Model;

use Generator;
use Keboola\AppsApiClient\Model\Deployment;
use Keboola\AppsApiClient\Tests\AppTestData;
use PHPUnit\Framework\TestCase;
use TypeError;

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
        self::assertNull($deployment->activeRunId);
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

    public function testFromResponseDataOfARunningDeployment(): void
    {
        $deployment = Deployment::fromResponseData(AppTestData::deployment([
            'runState' => 'running',
            'activeRunId' => AppTestData::RUN_ID,
        ]));

        self::assertSame(AppTestData::RUN_ID, $deployment->activeRunId);
    }

    /** A server that predates the field does not send it. */
    public function testAnAbsentActiveRunIdIsNull(): void
    {
        $data = AppTestData::deployment();
        unset($data['activeRunId']);

        self::assertNull(Deployment::fromResponseData($data)->activeRunId);
    }

    /**
     * @dataProvider wrongTypeProvider
     */
    public function testFromResponseDataRejectsWrongType(array $data): void
    {
        $this->expectException(TypeError::class);

        Deployment::fromResponseData($data);
    }

    public static function wrongTypeProvider(): Generator
    {
        yield 'id of wrong type' => [AppTestData::deployment(['id' => 1])];
        yield 'runState of wrong type' => [AppTestData::deployment(['runState' => null])];
        yield 'activeRunId of wrong type' => [AppTestData::deployment(['activeRunId' => 42])];
        yield 'archived of wrong type' => [AppTestData::deployment(['archived' => 0])];
        yield 'configVersion of wrong type' => [AppTestData::deployment(['configVersion' => 7])];
        yield 'archivedAt of wrong type' => [AppTestData::deployment(['archivedAt' => 1])];
    }
}
