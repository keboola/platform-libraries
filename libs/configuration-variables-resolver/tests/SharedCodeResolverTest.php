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
use Keboola\StorageApiBranch\Factory\ClientOptions;
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
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN'),
            ),
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
            $row->setRowId((string) $index);
            $row->setConfiguration($rowData);
            $rowIds[] = $components->addConfigurationRow($row)['id'];
        }
        return [$configId, $rowIds];
    }

    private function getSharedCodeResolver(): SharedCodeResolver
    {
        return new SharedCodeResolver($this->clientWrapper, $this->testLogger);
    }

    public function createBranch(string $branchName, ClientWrapper $clientWrapper): int
    {
        $branches = new DevBranches($clientWrapper->getBasicClient());
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
                'first_code' => ['code_content' => ['SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id']],
                // bwd compatible shared code configuration where the code is not array
                'secondCode' => ['code_content' => 'bar'],
            ],
        );
        $configuration = [
            'shared_code_id' => $sharedConfigurationId,
            'shared_code_row_ids' => $sharedCodeRowIds,
            'parameters' => [
                'simple_code' => ['{{secondCode}}'],
                'multiple_codes' => ['{{ first_code }}', 'and {{ secondCode }}.'],
                'non_replaced_code' => '{{secondCode}}',
                'child' => [
                    'also_non_replaced' => '{{secondCode}}',
                ],
                'mixed array' => [
                    '{{secondCode}}',
                    'keyed' => '{{secondCode}}',
                    '{{first_code}}',
                ],
                'Some Inline Variable' => ['some text {{some_var}} some text {{some_other_var}}'],
                'Variables With Code' => ['some text {{some_var}} some text {{some_other_var}} and {{first_code}}'],
                'Variables With 2 Codes' => [
                    'some text {{some_var}} some text {{some_other_var}} and {{first_code}} and {{secondCode}}',
                ],
                'Variables With 3 Codes' => [
                    'some text {{some_var}} some text {{some_other_var}} and {{first_code}} and {{secondCode}}',
                    '{{first_code}}',
                ],
            ],
        ];
        $sharedCodeResolver = $this->getSharedCodeResolver();
        $newConfiguration = $sharedCodeResolver->resolveSharedCode($configuration);
        self::assertEquals(
            [
                'parameters' => [
                    'simple_code' => ['bar'],
                    'multiple_codes' => [
                        'SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id',
                        'bar',
                    ],
                    'non_replaced_code' => '{{secondCode}}',
                    'child' => [
                        'also_non_replaced' => '{{secondCode}}',
                    ],
                    'mixed array' => [
                        '{{secondCode}}',
                        'keyed' => '{{secondCode}}',
                        '{{first_code}}',
                    ],
                    'Some Inline Variable' => [
                        'some text {{some_var}} some text {{some_other_var}}',
                    ],
                    'Variables With Code' => [
                        'SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id',
                    ],
                    'Variables With 2 Codes' => [
                        'SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id',
                        'bar',
                    ],
                    'Variables With 3 Codes' => [
                        'SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id',
                        'bar',
                        'SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id',
                    ],
                ],
                'shared_code_id' => $sharedConfigurationId,
                'shared_code_row_ids' => ['first_code', 'secondCode'],
            ],
            $newConfiguration,
        );
        self::assertTrue(
            $this->testLogger->hasInfoThatContains('Loaded shared code snippets with ids: "first_code, secondCode".'),
        );
    }

    public function testResolveSharedCodeNoConfiguration(): void
    {
        list ($sharedConfigurationId, $sharedCodeRowIds) = $this->createSharedCodeConfiguration(
            $this->clientWrapper->getBasicClient(),
            [
                'first_code' => ['code_content' => 'SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id'],
                'secondCode' => ['code_content' => 'bar'],
            ],
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
                'shared_code_row_ids' => ['first_code', 'secondCode'],
            ],
            $newConfiguration,
        );
        self::assertFalse(
            $this->testLogger->hasInfoThatContains('Loaded shared code snippets with ids: "first_code, secondCode".'),
        );
    }

    public function testResolveSharedCodeNoRows(): void
    {
        list ($sharedConfigurationId, $sharedCodeRowIds) = $this->createSharedCodeConfiguration(
            $this->clientWrapper->getBasicClient(),
            [
                'first_code' => ['code_content' => 'SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id'],
                'secondCode' => ['code_content' => 'bar'],
            ],
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
            $newConfiguration,
        );
        self::assertFalse(
            $this->testLogger->hasInfoThatContains('Loaded shared code snippets with ids: "first_code, secondCode".'),
        );
    }

    public function testResolveSharedCodeNonExistentConfiguration(): void
    {
        list ($sharedConfigurationId, $sharedCodeRowIds) = $this->createSharedCodeConfiguration(
            $this->clientWrapper->getBasicClient(),
            [
                'first_code' => ['code_content' => 'SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id'],
                'secondCode' => ['code_content' => 'bar'],
            ],
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
            'Shared code configuration cannot be read: Configuration non-existent not found',
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
            ],
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
            'Shared code configuration cannot be read: Row foo not found',
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
            ],
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
            'Shared code configuration is invalid: Unrecognized option "this is broken" under "configuration"',
        );
        $sharedCodeResolver->resolveSharedCode($configuration);
    }

    public function testResolveSharedCodeBranch(): void
    {
        list ($sharedConfigurationId, $sharedCodeRowIds) = $this->createSharedCodeConfiguration(
            $this->clientWrapper->getBasicClient(),
            [
                'first_code' => ['code_content' => ['SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id']],
                'secondCode' => ['code_content' => ['bar']],
            ],
        );
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
            ),
        );
        $branchId = $this->createBranch('my-dev-branch', $clientWrapper);
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN'),
                (string) $branchId,
            ),
        );

        // modify the dev branch shared code configuration to "dev-bar"
        $components = new Components($this->clientWrapper->getBranchClient());
        $configuration = new StorageConfiguration();
        $configuration->setComponentId('keboola.shared-code');
        $configuration->setConfigurationId($sharedConfigurationId);
        $newRow = new ConfigurationRow($configuration);
        $newRow->setRowId($sharedCodeRowIds[1]);
        $newRow->setConfiguration(['code_content' => ['dev-bar']]);
        $components->updateConfigurationRow($newRow);

        $configuration = [
            'shared_code_id' => $sharedConfigurationId,
            'shared_code_row_ids' => $sharedCodeRowIds,
            'parameters' => [
                'some_parameter' => 'foo is {{ foo }} and {{non-existent}} and {{ first_code }} and {{ secondCode }} .',
                'some_other_parameter' => [
                    'foo is {{ foo }} and {{non-existent}} and {{ first_code }} and {{ secondCode }} .',
                ],
            ],
        ];
        $sharedCodeResolver = $this->getSharedCodeResolver();
        $newConfiguration = $sharedCodeResolver->resolveSharedCode($configuration);
        self::assertEquals(
            [
                'parameters' => [
                    'some_parameter' =>
                        'foo is {{ foo }} and {{non-existent}} and {{ first_code }} and {{ secondCode }} .',
                    'some_other_parameter' => [
                        'SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id',
                        'dev-bar',
                    ],
                ],
                'shared_code_id' => $sharedConfigurationId,
                'shared_code_row_ids' => [0 => 'first_code', 1 => 'secondCode'],
            ],
            $newConfiguration,
        );
        self::assertTrue(
            $this->testLogger->hasInfoThatContains('Loaded shared code snippets with ids: "first_code, secondCode".'),
        );
    }

    public function testResolveSharedCodeNumericIds(): void
    {
        list ($sharedConfigurationId, $sharedCodeRowIds) = $this->createSharedCodeConfiguration(
            $this->clientWrapper->getBasicClient(),
            [
                '123456' => ['code_content' => ['SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id']],
                // bwd compatible shared code configuration where the code is not array
                1234567 => ['code_content' => 'bar'],
            ],
        );
        $configuration = [
            'shared_code_id' => $sharedConfigurationId,
            'shared_code_row_ids' => $sharedCodeRowIds,
            'parameters' => [
                'first code' => ['{{123456}}'],
                'second code' => ['{{1234567}}'],
            ],
        ];
        $sharedCodeResolver = $this->getSharedCodeResolver();
        $newConfiguration = $sharedCodeResolver->resolveSharedCode($configuration);
        self::assertEquals(
            [
                'parameters' => [
                    'first code' => ['SELECT * FROM {{tab1}} LEFT JOIN {{tab2}} ON b.a_id = a.id'],
                    'second code' => ['bar'],
                ],
                'shared_code_id' => $sharedConfigurationId,
                'shared_code_row_ids' => [
                    '123456',
                    '1234567',
                ],
            ],
            $newConfiguration,
        );
        self::assertTrue(
            $this->testLogger->hasInfoThatContains('Loaded shared code snippets with ids: "123456, 1234567".'),
        );
    }
}
