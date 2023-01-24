<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\State;

use Keboola\InputMapping\State\InputFileState;
use PHPUnit\Framework\TestCase;

class InputFileStateTest extends TestCase
{
    public function testGetTags(): void
    {
        $configuration = [
            'tags' => [
                [
                    'name' => 'test',
                    'match' => 'include',
                ],
            ],
            'lastImportId' => '12345',
        ];
        $state = new InputFileState($configuration);
        self::assertEquals([['name' => 'test', 'match' => 'include']], $state->getTags());
    }

    public function testGetLastImportId(): void
    {
        $configuration = [
            'tags' => [
                [
                    'name' => 'test',
                    'match' => 'include',
                ],
            ],
            'lastImportId' => '12345',
        ];
        $state = new InputFileState($configuration);
        self::assertEquals('12345', $state->getLastImportId());
    }

    public function testJsonSerialize(): void
    {
        $configuration = [
            'tags' => [
                [
                    'name' => 'test',
                    'match' => 'include',
                ],
            ],
            'lastImportId' => '12345',
        ];
        $state = new InputFileState($configuration);
        self::assertEquals($configuration, $state->jsonSerialize());
    }
}
