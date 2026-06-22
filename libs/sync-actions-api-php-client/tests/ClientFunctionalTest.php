<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient\Tests;

use Keboola\SyncActionsClient\ActionData;
use Keboola\SyncActionsClient\Exception\SyncActionsClientException;
use Keboola\SyncActionsClient\SyncActionsApiClient;
use PHPUnit\Framework\TestCase;
use Webmozart\Assert\Assert;

class ClientFunctionalTest extends TestCase
{
    private const string COMPONENT_ID = 'keboola.runner-config-test';

    public function testGetActions(): void
    {
        $client = $this->getClient();

        $actions = $client->getActions(self::COMPONENT_ID);
        self::assertSame(
            [
                'dumpConfig',
                'dumpEnv',
                'timeout',
                'emptyJsonArray',
                'emptyJsonObject',
                'invalidJson',
                'noResponse',
                'userError',
                'applicationError',
                'printLogs',
            ],
            $actions->actions,
        );
    }

    public function testCallAction(): void
    {
        $client = $this->getClient();
        $response = $client->callAction(new ActionData(
            'keboola.runner-config-test',
            'dumpConfig',
            [
                'parameters' => [
                    'arbitrary' => 'bar',
                ],
            ],
        ));

        self::assertObjectHasProperty('parameters', $response->data);
    }

    public function testInvalidComponent(): void
    {
        $client = $this->getClient();

        $this->expectException(SyncActionsClientException::class);
        $this->expectExceptionMessage('Component "non-existent-component" not found');
        $client->callAction(new ActionData('non-existent-component', 'non-existent-action', []));
    }

    public function testInvalidAction(): void
    {
        $client = $this->getClient();

        $this->expectException(SyncActionsClientException::class);
        $this->expectExceptionMessage(sprintf(
            'Action "non-existent-action" not defined for component "%s".',
            self::COMPONENT_ID,
        ));
        $client->callAction(new ActionData(self::COMPONENT_ID, 'non-existent-action', []));
    }

    private function getClient(): SyncActionsApiClient
    {
        $baseUrl = sprintf('https://sync-actions.%s', getenv('HOSTNAME_SUFFIX'));
        $token = (string) getenv('STORAGE_API_TOKEN');
        Assert::stringNotEmpty($token);

        return new SyncActionsApiClient($baseUrl, $token);
    }
}
