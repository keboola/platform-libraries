<?php

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Writer\Table\TableWriterFactory;
use Keboola\OutputMapping\Writer\Table\TableWriterV1;
use Keboola\OutputMapping\Writer\Table\TableWriterV2;
use Keboola\StorageApi\Client;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class TableWriterFactoryTest extends TestCase
{
    /**
     * @dataProvider provideTokensWithFeatures
     */
    public function testCorrectWriterIsCreatedBasedOnTokenFeatures(array $owner, $expectedWriterClass)
    {
        $sapiClient = $this->createMock(Client::class);
        $sapiClient->expects(self::once())->method('verifyToken')->willReturn([
            'owner' => $owner,
        ]);

        $strategyFactory = new StrategyFactory(
            new ClientWrapper(
                $sapiClient,
                null,
                new NullLogger(),
                ''
            ),
            new NullLogger(),
            'json'
        );

        $factory = new TableWriterFactory($strategyFactory);
        $writer = $factory->createTableWriter();

        self::assertSame($expectedWriterClass, get_class($writer));
    }

    public function provideTokensWithFeatures()
    {
        return [
            'empty features' => [
                ['features' => []],
                TableWriterV1::class,
            ],

            'other features' => [
                ['features' => ['other-feature']],
                TableWriterV1::class,
            ],

            'new OM feature' => [
                ['features' => ['new-table-output-mapping']],
                TableWriterV2::class,
            ],
        ];
    }
}
