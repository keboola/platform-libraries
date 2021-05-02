<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Tests;

use Keboola\ConfigurationVariablesResolver\Exception\UserException;
use Keboola\ConfigurationVariablesResolver\SharedCodeResolver;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\Components\Configuration as StorageConfiguration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Psr\Log\Test\TestLogger;

class SharedCodeResolverTest extends TestCase
{
    private ClientWrapper $clientWrapper;

    private TestLogger $testLogger;

    public function setUp(): void
    {
        parent::setUp();

        $this->clientWrapper = new ClientWrapper(
            new Client([
                'url' => getenv('STORAGE_API_URL'),
                'token' => getenv('STORAGE_API_TOKEN'),
            ]),
            null,
            new NullLogger(),
            ''
        );
        $components = new Components($this->clientWrapper->getBasicClient());
        $listOptions = new ListComponentConfigurationsOptions();
        $listOptions->setComponentId('keboola.shared-code');
        $configurations = $components->listComponentConfigurations($listOptions);
        foreach ($configurations as $configuration) {
            $components->deleteConfiguration('keboola.shared-code', $configuration['id']);
        }

        $this->testLogger = new TestLogger();
    }

    private function createSharedCodeConfiguration(Client $client, array $rowsData): array
    {
        $components = new Components($client);
        $configuration = new StorageConfiguration();
        $configuration->setComponentId('keboola.shared-code');
        $configuration->setName('runner-test');
        $configuration->setConfiguration(['componentId' => 'keboola.snowflake-transformation']);
        $configId = $components->addConfiguration($configuration)['id'];
        $configuration->setConfigurationId($configId);
        $rowIds = [];
        foreach ($rowsData as $index => $rowData) {
            $row = new ConfigurationRow($configuration);
            $row->setName('runner-test');
            $row->setRowId($index);
            $row->setConfiguration($rowData);
            $rowIds[] = $components->addConfigurationRow($row)['id'];
        }
        return [$configId, $rowIds];
    }

    private function getSharedCodeResolver(): SharedCodeResolver
    {
        return new SharedCodeResolver($this->clientWrapper, $this->testLogger);
    }

    public function createBranch(string $branchName): string
    {
        $branches = new DevBranches($this->clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === $branchName) {
                $branches->deleteBranch($branch['id']);
            }
        }
        return $branches->createBranch($branchName)['id'];
    }

    public function testResolveSharedCode(): void
    {
        list ($sharedConfigurationId, $sharedCodeRowIds) = $this->createSharedCodeConfiguration(
            $this->clientWrapper->getBasicClient(),
            [
                'first_code' => ['code_content' => 'SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id'],
                'secondCode' => ['code_content' => 'bar'],
            ]
        );
        $configuration = [
            'shared_code_id' => $sharedConfigurationId,
            'shared_code_row_ids' => $sharedCodeRowIds,
            'parameters' => [
                'some_parameter' => 'foo is {{ foo }} and {{non-existent}} and {{ first_code }} and {{ secondCode }} .',
            ],
        ];
        $sharedCodeResolver = $this->getSharedCodeResolver();
        $newConfiguration = $sharedCodeResolver->resolveSharedCode($configuration);
        self::assertEquals(
            [
                'parameters' => [
                    'some_parameter' =>
                        'foo is {{ foo }} and {{ non-existent }} and ' .
                        'SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id and bar .',
                ],
                'shared_code_id' => $sharedConfigurationId,
                'shared_code_row_ids' => [0 => 'first_code', 1 => 'secondCode'],
            ],
            $newConfiguration,
        );
        self::assertTrue(
            $this->testLogger->hasInfoThatContains('Loaded shared code snippets with ids: "first_code, secondCode".')
        );
    }

    public function testResolveSharedCodeNoConfiguration(): void
    {
        list ($sharedConfigurationId, $sharedCodeRowIds) = $this->createSharedCodeConfiguration(
            $this->clientWrapper->getBasicClient(),
            [
                'first_code' => ['code_content' => 'SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id'],
                'secondCode' => ['code_content' => 'bar'],
            ]
        );
        $configuration = [
            'shared_code_row_ids' => $sharedCodeRowIds,
            'parameters' => [
                'some_parameter' => 'foo is {{ foo }} and {{non-existent}} and {{ first_code }} and {{ secondCode }} .',
            ],
        ];
        $sharedCodeResolver = $this->getSharedCodeResolver();
        $newConfiguration = $sharedCodeResolver->resolveSharedCode($configuration);
        self::assertEquals(
            [
                'parameters' => [
                    'some_parameter' =>
                        'foo is {{ foo }} and {{non-existent}} and {{ first_code }} and {{ secondCode }} .',
                ],
                'shared_code_row_ids' => [0 => 'first_code', 1 => 'secondCode'],
            ],
            $newConfiguration
        );
        self::assertFalse(
            $this->testLogger->hasInfoThatContains('Loaded shared code snippets with ids: "first_code, secondCode".')
        );
    }

    public function testResolveSharedCodeNoRows(): void
    {
        list ($sharedConfigurationId, $sharedCodeRowIds) = $this->createSharedCodeConfiguration(
            $this->clientWrapper->getBasicClient(),
            [
                'first_code' => ['code_content' => 'SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id'],
                'secondCode' => ['code_content' => 'bar'],
            ]
        );
        $configuration = [
            'shared_code_id' => $sharedConfigurationId,
            'parameters' => [
                'some_parameter' => 'foo is {{ foo }} and {{non-existent}} and {{ first_code }} and {{ secondCode }} .',
            ],
        ];
        $sharedCodeResolver = $this->getSharedCodeResolver();
        $newConfiguration = $sharedCodeResolver->resolveSharedCode($configuration);
        self::assertEquals(
            [
                'parameters' => [
                    'some_parameter' =>
                        'foo is {{ foo }} and {{non-existent}} and {{ first_code }} and {{ secondCode }} .',
                ],
                'shared_code_id' => $sharedConfigurationId,
            ],
            $newConfiguration
        );
        self::assertFalse(
            $this->testLogger->hasInfoThatContains('Loaded shared code snippets with ids: "first_code, secondCode".')
        );
    }

    public function testResolveSharedCodeNonExistentConfiguration(): void
    {
        list ($sharedConfigurationId, $sharedCodeRowIds) = $this->createSharedCodeConfiguration(
            $this->clientWrapper->getBasicClient(),
            [
                'first_code' => ['code_content' => 'SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id'],
                'secondCode' => ['code_content' => 'bar'],
            ]
        );
        $configuration = [
            'shared_code_id' => 'non-existent',
            'shared_code_row_ids' => $sharedCodeRowIds,
            'parameters' => [
                'some_parameter' => 'foo is {{ foo }} and {{non-existent}} and {{ first_code }} and {{ secondCode }} .',
            ],
        ];
        $sharedCodeResolver = $this->getSharedCodeResolver();
        self::expectException(UserException::class);
        self::expectExceptionMessage(
            'Shared code configuration cannot be read: Configuration non-existent not found'
        );
        $sharedCodeResolver->resolveSharedCode($configuration);
    }

    public function testResolveSharedCodeNonExistentRow(): void
    {
        list ($sharedConfigurationId, $sharedCodeRowIds) = $this->createSharedCodeConfiguration(
            $this->clientWrapper->getBasicClient(),
            [
                'first_code' => ['code_content' => 'SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id'],
                'secondCode' => ['code_content' => 'bar'],
            ]
        );
        $configuration = [
            'shared_code_id' => $sharedConfigurationId,
            'shared_code_row_ids' => ['foo', 'bar'],
            'parameters' => [
                'some_parameter' => 'foo is {{ foo }} and {{non-existent}} and {{ first_code }} and {{ secondCode }} .',
            ],
        ];
        $sharedCodeResolver = $this->getSharedCodeResolver();
        self::expectException(UserException::class);
        self::expectExceptionMessage(
            'Shared code configuration cannot be read: Row foo not found'
        );
        $sharedCodeResolver->resolveSharedCode($configuration);
    }

    public function testResolveSharedCodeInvalidRow(): void
    {
        list ($sharedConfigurationId, $sharedCodeRowIds) = $this->createSharedCodeConfiguration(
            $this->clientWrapper->getBasicClient(),
            [
                'first_code' => ['this is broken' => 'SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id'],
                'secondCode' => ['code_content' => 'bar'],
            ]
        );
        $configuration = [
            'shared_code_id' => $sharedConfigurationId,
            'shared_code_row_ids' => $sharedCodeRowIds,
            'parameters' => [
                'some_parameter' => 'foo is {{ foo }} and {{non-existent}} and {{ first_code }} and {{ secondCode }} .',
            ],
        ];
        $sharedCodeResolver = $this->getSharedCodeResolver();
        self::expectException(UserException::class);
        self::expectExceptionMessage(
            'Shared code configuration is invalid: Unrecognized option "this is broken" under "configuration"'
        );
        $sharedCodeResolver->resolveSharedCode($configuration);
    }

    public function testResolveSharedCodeBranch(): void
    {
        $client = new Client([
            'url' => getenv('STORAGE_API_URL'),
            'token' => getenv('STORAGE_API_TOKEN_MASTER'),
        ]);

        list ($sharedConfigurationId, $sharedCodeRowIds) = $this->createSharedCodeConfiguration(
            $this->clientWrapper->getBasicClient(),
            [
                'first_code' => ['code_content' => 'SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id'],
                'secondCode' => ['code_content' => 'bar'],
            ]
        );
        $this->clientWrapper = new ClientWrapper(
            $client,
            null,
            new NullLogger()
        );
        $branchId = $this->createBranch('my-dev-branch');
        $this->clientWrapper->setBranchId($branchId);

        // modify the dev branch shared code configuration to "dev-bar"
        $components = new Components($this->clientWrapper->getBranchClient());
        $configuration = new StorageConfiguration();
        $configuration->setComponentId('keboola.shared-code');
        $configuration->setConfigurationId($sharedConfigurationId);
        $newRow = new ConfigurationRow($configuration);
        $newRow->setRowId($sharedCodeRowIds[1]);
        $newRow->setConfiguration(['code_content' => 'dev-bar']);
        $components->updateConfigurationRow($newRow);

        $configuration = [
            'shared_code_id' => $sharedConfigurationId,
            'shared_code_row_ids' => $sharedCodeRowIds,
            'parameters' => [
                'some_parameter' => 'foo is {{ foo }} and {{non-existent}} and {{ first_code }} and {{ secondCode }} .',
            ],
        ];
        $sharedCodeResolver = $this->getSharedCodeResolver();
        $newConfiguration = $sharedCodeResolver->resolveSharedCode($configuration);
        self::assertEquals(
            [
                'parameters' => [
                    'some_parameter' =>
                        'foo is {{ foo }} and {{ non-existent }} and ' .
                        'SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id and dev-bar .',
                ],
                'shared_code_id' => $sharedConfigurationId,
                'shared_code_row_ids' => [0 => 'first_code', 1 => 'secondCode'],
            ],
            $newConfiguration
        );
        self::assertTrue(
            $this->testLogger->hasInfoThatContains('Loaded shared code snippets with ids: "first_code, secondCode".')
        );
    }
}
