<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Result;

use Keboola\InputMapping\Table\Result\MetadataItem;
use PHPUnit\Framework\TestCase;

class MetadataItemTest extends TestCase
{
    public function testAccessors(): void
    {
        $metadataItem = new MetadataItem([
            'key' => 'key1',
            'value' => 'value1',
            'provider' => 'provider1',
            'timestamp' => 'timestamp1',
        ]);
        self::assertSame('key1', $metadataItem->getKey());
        self::assertSame('value1', $metadataItem->getValue());
        self::assertSame('provider1', $metadataItem->getProvider());
        self::assertSame('timestamp1', $metadataItem->getTimestamp());
    }
}
