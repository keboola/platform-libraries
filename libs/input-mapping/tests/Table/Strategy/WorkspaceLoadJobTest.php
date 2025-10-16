<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Generator;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\WorkspaceLoadJob;
use Keboola\InputMapping\Table\Strategy\WorkspaceLoadType;
use PHPUnit\Framework\TestCase;

class WorkspaceLoadJobTest extends TestCase
{
    /**
     * @dataProvider validJobTypeProvider
     */
    public function testConstructorWithValidJobTypes(WorkspaceLoadType $jobType): void
    {
        $this->expectNotToPerformAssertions();

        $tableOptions = $this->createMock(RewrittenInputTableOptions::class);
        new WorkspaceLoadJob('123', $jobType, [$tableOptions]);
    }

    /**
     * @dataProvider invalidJobTypeProvider
     */
    public function testConstructorThrowsExceptionForInvalidJobTypes(WorkspaceLoadType $jobType): void
    {
        $this->expectException(InputOperationException::class);
        $this->expectExceptionMessage(
            sprintf('Invalid job type "%s". Only CLONE and COPY are allowed for jobs.', $jobType->value),
        );

        $tableOptions = $this->createMock(RewrittenInputTableOptions::class);
        new WorkspaceLoadJob('789', $jobType, [$tableOptions]);
    }

    public function testConstructorWithNullJobType(): void
    {
        $tableOptions = $this->createMock(RewrittenInputTableOptions::class);
        $job = new WorkspaceLoadJob('123', null, [$tableOptions]);

        self::assertNull($job->jobType);
        self::assertTrue($job->isMixedTypeJob());
    }

    public function testIsMixedTypeJobReturnsFalseForSpecificTypes(): void
    {
        $tableOptions = $this->createMock(RewrittenInputTableOptions::class);

        $cloneJob = new WorkspaceLoadJob('123', WorkspaceLoadType::CLONE, [$tableOptions]);
        self::assertFalse($cloneJob->isMixedTypeJob());

        $copyJob = new WorkspaceLoadJob('456', WorkspaceLoadType::COPY, [$tableOptions]);
        self::assertFalse($copyJob->isMixedTypeJob());
    }

    public static function validJobTypeProvider(): Generator
    {
        yield 'CLONE job type' => [WorkspaceLoadType::CLONE];
        yield 'COPY job type' => [WorkspaceLoadType::COPY];
    }

    public static function invalidJobTypeProvider(): Generator
    {
        yield 'VIEW job type' => [WorkspaceLoadType::VIEW];
    }
}
