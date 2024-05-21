<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Generator;
use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyInputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Writer\TableWriter;

class TableDefinitionV2Test extends AbstractTestCase
{
    public function setUp(): void
    {
        parent::setUp();

        $requiredFeatures = [
            'new-native-types',
        ];

        $tokenData = $this->clientWrapper->getBranchClient()->verifyToken();
        foreach ($requiredFeatures as $requiredFeature) {
            if (!in_array($requiredFeature, $tokenData['owner']['features'])) {
                self::fail(sprintf(
                    '%s is not enabled for project "%s".',
                    ucfirst(str_replace('-', ' ', $requiredFeature)),
                    $tokenData['owner']['id'],
                ));
            }
        }
    }

    public function testTemporaryDeleteMe(): void
    {
        self::assertTrue(true);
    }
}
