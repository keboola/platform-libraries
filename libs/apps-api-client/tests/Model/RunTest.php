<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient\Tests\Model;

use Generator;
use Keboola\AppsApiClient\Model\Run;
use Keboola\AppsApiClient\Tests\AppTestData;
use PHPUnit\Framework\TestCase;
use TypeError;

class RunTest extends TestCase
{
    public function testFromResponseData(): void
    {
        $run = Run::fromResponseData(AppTestData::run());

        self::assertSame(AppTestData::RUN_ID, $run->id);
        self::assertSame('123', $run->appId);
        self::assertSame(Run::STATE_RUNNING, $run->state);
        self::assertSame('2026-11-20T10:00:00+00:00', $run->createdAt);
        self::assertSame('2026-11-20T10:00:30+00:00', $run->startedAt);
        self::assertNull($run->stoppedAt);
        self::assertNull($run->failureReason);
    }

    public function testFromResponseDataOfFailedRun(): void
    {
        $run = Run::fromResponseData(AppTestData::run([
            'state' => 'failed',
            'startedAt' => null,
            'stoppedAt' => '2026-11-20T10:05:00+00:00',
            'failureReason' => ['reason' => 'startupProbeFailed', 'message' => 'The app did not start in time.'],
        ]));

        self::assertSame(Run::STATE_FAILED, $run->state);
        self::assertNull($run->startedAt);
        self::assertNotNull($run->failureReason);
        self::assertSame('startupProbeFailed', $run->failureReason->reason);
        self::assertSame('The app did not start in time.', $run->failureReason->message);
    }

    public function testFromResponseDataOfRunWithoutStateOrDeployment(): void
    {
        $run = Run::fromResponseData(AppTestData::run([
            'state' => null,
            'createdAt' => null,
            'deploymentId' => null,
            'subject' => null,
            'failureReason' => ['reason' => null, 'message' => null],
        ]));

        self::assertNull($run->state);
        self::assertNull($run->createdAt);
        self::assertNotNull($run->failureReason);
        self::assertNull($run->failureReason->reason);
        self::assertNull($run->failureReason->message);
    }

    /**
     * @dataProvider wrongTypeProvider
     */
    public function testFromResponseDataRejectsWrongType(array $data): void
    {
        $this->expectException(TypeError::class);

        Run::fromResponseData($data);
    }

    public static function wrongTypeProvider(): Generator
    {
        yield 'id of wrong type' => [AppTestData::run(['id' => 1])];
        yield 'state of wrong type' => [AppTestData::run(['state' => 1])];
        yield 'failureReason message of wrong type' => [
            AppTestData::run(['failureReason' => ['reason' => 'x', 'message' => 1]]),
        ];
    }
}
