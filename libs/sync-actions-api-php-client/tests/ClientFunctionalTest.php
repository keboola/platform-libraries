<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient\Tests;

use Keboola\SyncActionsClient\ActionData;
use Keboola\SyncActionsClient\Client;
use Keboola\SyncActionsClient\Exception\ClientException;
use PHPUnit\Framework\TestCase;

class ClientFunctionalTest extends TestCase
{
    private const COMPONENT_ID = 'keboola.ex-db-snowflake';
    private const COMPONENT_ID_2 = 'keboola.ex-onedrive';

    public function testGetActions(): void
    {
        $client = $this->getClient();

        $actions = $client->getActions(self::COMPONENT_ID);
        self::assertEquals([
            'actions' => [
                'testConnection',
                'getTables',
            ],
        ], $actions);

        $actions = $client->getActions(self::COMPONENT_ID_2);
        self::assertEquals([
            'actions' => [
                'search',
                'getWorksheets',
            ],
        ], $actions);
    }

    public function testCallAction(): void
    {
        $client = $this->getClient();
        $response = $client->callAction(new ActionData(
            'keboola.ex-currency',
            'list',
            [
                'parameters' => [
                    'dataset' => 'in.c-ex-currency-test',
                ],
            ],
        ));

        self::assertArrayHasKey('datasets', $response);
    }

    public function testInvalidComponent(): void
    {
        $client = $this->getClient();

        self::expectException(ClientException::class);
        self::expectExceptionMessage('Component \"non-existent-component\" not found');
        $client->callAction(new ActionData('non-existent-component', 'unexistAction', []));
    }

    public function testInvalidAction(): void
    {
        $client = $this->getClient();

        self::expectException(ClientException::class);
        self::expectExceptionMessage(sprintf(
            'Action \"non-existent-action\" not defined for component \"%s\".',
            self::COMPONENT_ID,
        ));
        $client->callAction(new ActionData(self::COMPONENT_ID, 'non-existent-action', []));
    }

    private function getClient(): Client
    {
        return new Client(
            (string) getenv('KBC_SYNC_ACTIONS_URL'),
            (string) getenv('KBC_TOKEN'),
        );
    }
}
