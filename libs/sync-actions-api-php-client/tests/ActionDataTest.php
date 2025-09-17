<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient\Tests;

use Keboola\SyncActionsClient\ActionData;
use PHPUnit\Framework\TestCase;

class ActionDataTest extends TestCase
{
    public function testAccessorsMin(): void
    {
        $jobData = new ActionData('dummy', 'action');
        self::assertEquals(
            [
                'componentId' => 'dummy',
                'action' => 'action',
                'configData' => [],
            ],
            $jobData->getArray(),
        );
    }

    public function testAccessorsFull(): void
    {
        $jobData = new ActionData(
            'dummy',
            'action',
            ['foo' => 'bar'],
            '1.2.3',
            '123456',
        );

        self::assertEquals(
            [
                'componentId' => 'dummy',
                'action' => 'action',
                'tag' => '1.2.3',
                'branchId' => '123456',
                'configData' => ['foo' => 'bar'],
            ],
            $jobData->getArray(),
        );
    }
}
