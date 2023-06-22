<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient\Tests\Variables;

use GuzzleHttp\Psr7\Response;
use Keboola\VaultApiClient\ApiClientConfiguration;
use Keboola\VaultApiClient\Json;
use Keboola\VaultApiClient\Tests\ApiClientTestTrait;
use Keboola\VaultApiClient\Variables\Model\ListOptions;
use Keboola\VaultApiClient\Variables\Model\Variable;
use Keboola\VaultApiClient\Variables\VariablesApiClient;
use PHPUnit\Framework\TestCase;

class VariablesApiClientTest extends TestCase
{
    use ApiClientTestTrait;

    private const BASE_URL = 'https://vault.keboola.com';
    private const API_TOKEN = 'my-token';

    public function testCreateVariable(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                [
                    'Content-Type' => 'application/json',
                ],
                Json::encodeArray([
                    'hash' => 'hash',
                    'key' => 'key',
                    'value' => 'val',
                    'isEncrypted' => true,
                    'attributes' => [
                        'branchId' => '123',
                    ],
                ])
            ),
        ]);

        $client = new VariablesApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            new ApiClientConfiguration(
                requestHandler: $requestHandler(...),
            ),
        );

        $variable = $client->createVariable(
            'key',
            'val',
            true,
            [
                'branchId' => '123',
            ]
        );

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'POST',
            self::BASE_URL . '/variables',
            [
                'Content-Type' => 'application/json',
                'X-StorageApi-Token' => self::API_TOKEN,
            ],
            Json::encodeArray([
                'key' => 'key',
                'value' => 'val',
                'isEncrypted' => true,
                'attributes' => [
                    'branchId' => '123',
                ],
            ]),
            $requestsHistory[0]['request'],
        );

        self::assertEquals(
            new Variable(
                hash: 'hash',
                key: 'key',
                value: 'val',
                isEncrypted: true,
                attributes: [
                    'branchId' => '123',
                ],
            ),
            $variable,
        );
    }

    public function testDeleteVariable(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(200),
        ]);

        $client = new VariablesApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            new ApiClientConfiguration(
                requestHandler: $requestHandler(...),
            ),
        );

        $client->deleteVariable('hash');

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'DELETE',
            self::BASE_URL . '/variables/hash',
            [
                'X-StorageApi-Token' => self::API_TOKEN,
            ],
            null,
            $requestsHistory[0]['request'],
        );
    }

    public function testListVariables(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                [
                    'Content-Type' => 'application/json',
                ],
                Json::encodeArray([
                    ['hash' => 'hash1', 'key' => 'key1', 'value' => 'val1', 'isEncrypted' => false, 'attributes' => []],
                    ['hash' => 'hash2', 'key' => 'key2', 'value' => 'val2', 'isEncrypted' => false, 'attributes' => []],
                ])
            ),
        ]);

        $client = new VariablesApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            new ApiClientConfiguration(
                requestHandler: $requestHandler(...),
            ),
        );

        $variables = $client->listVariables(new ListOptions(offset: 5));

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'GET',
            self::BASE_URL . '/variables?offset=5',
            [
                'X-StorageApi-Token' => self::API_TOKEN,
            ],
            null,
            $requestsHistory[0]['request'],
        );

        self::assertCount(2, $variables);
        self::assertEquals(
            new Variable(
                hash: 'hash1',
                key: 'key1',
                value: 'val1',
                isEncrypted: false,
                attributes: [],
            ),
            $variables[0],
        );
        self::assertEquals(
            new Variable(
                hash: 'hash2',
                key: 'key2',
                value: 'val2',
                isEncrypted: false,
                attributes: [],
            ),
            $variables[1],
        );
    }

    public function testListMergedVariablesFor(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                [
                    'Content-Type' => 'application/json',
                ],
                Json::encodeArray([
                    ['hash' => 'hash1', 'key' => 'key1', 'value' => 'val1', 'isEncrypted' => false, 'attributes' => []],
                    ['hash' => 'hash2', 'key' => 'key2', 'value' => 'val2', 'isEncrypted' => false, 'attributes' => []],
                ])
            ),
        ]);

        $client = new VariablesApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            new ApiClientConfiguration(
                requestHandler: $requestHandler(...),
            ),
        );

        $variables = $client->listMergedVariablesForBranch('123');

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'GET',
            self::BASE_URL . '/variables/merged/branch?branchId=123',
            [
                'X-StorageApi-Token' => self::API_TOKEN,
            ],
            null,
            $requestsHistory[0]['request'],
        );

        self::assertCount(2, $variables);
        self::assertEquals(
            new Variable(
                hash: 'hash1',
                key: 'key1',
                value: 'val1',
                isEncrypted: false,
                attributes: [],
            ),
            $variables[0],
        );
        self::assertEquals(
            new Variable(
                hash: 'hash2',
                key: 'key2',
                value: 'val2',
                isEncrypted: false,
                attributes: [],
            ),
            $variables[1],
        );
    }
}
