<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Tests;

use Keboola\ConfigurationVariablesResolver\ComponentsClientHelper;
use Keboola\ConfigurationVariablesResolver\Exception\UserException;
use Keboola\ConfigurationVariablesResolver\VariablesResolver;
use Keboola\StorageApi\ClientException as StorageApiClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\Components\Configuration as StorageConfiguration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\VaultApiClient\Variables\Model\ListOptions;
use Keboola\VaultApiClient\Variables\VariablesApiClient;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class VariablesResolverFunctionalTest extends TestCase
{
    use TestEnvVarsTrait;

    private const CONFIG_ID = 'variables-resolver-test';
    private const CONFIG_ROW_ID = 'variables-resolver-test-row';

    private readonly TestHandler $logsHandler;
    private readonly LoggerInterface $logger;
    private readonly ClientWrapper $clientWrapper;
    private readonly VariablesApiClient $variablesApiClient;

    /** @var non-empty-string */
    private string $mainBranchId;

    public function setUp(): void
    {
        parent::setUp();

        $this->logsHandler = new TestHandler();
        $this->logger = new Logger('tests', [$this->logsHandler]);

        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                self::getRequiredEnv('STORAGE_API_URL'),
                self::getRequiredEnv('STORAGE_API_TOKEN'),
            )
        );

        $this->variablesApiClient = new VariablesApiClient(
            self::getRequiredEnv('VAULT_API_URL'),
            self::getRequiredEnv('STORAGE_API_TOKEN'),
        );

        $branchesApiClient = new DevBranches($this->clientWrapper->getBranchClientIfAvailable());
        $branches = $branchesApiClient->listBranches();
        foreach ($branches as $branch) {
            if ($branch['isDefault']) {
                $branchId = (string) $branch['id'];
                self::assertNotEmpty($branchId);

                $this->mainBranchId = $branchId;
                break;
            }
        }
        self::assertTrue(isset($this->mainBranchId), 'Main branch not found.');
    }

    private function setupConfigurationVariables(array $data, array $rowData): void
    {
        $components = new Components($this->clientWrapper->getBranchClientIfAvailable());

        try {
            $components->deleteConfiguration(ComponentsClientHelper::KEBOOLA_VARIABLES, self::CONFIG_ID);
        } catch (StorageApiClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }

        $configuration = new StorageConfiguration();
        $configuration->setComponentId(ComponentsClientHelper::KEBOOLA_VARIABLES);
        $configuration->setName('variables-resolver-test');
        $configuration->setConfiguration($data);
        $configuration->setConfigurationId(self::CONFIG_ID);
        $components->addConfiguration($configuration);

        $row = new ConfigurationRow($configuration);
        $row->setName('variables-resolver-test-row');
        $row->setConfiguration($rowData);
        $row->setRowId(self::CONFIG_ROW_ID);
        $components->addConfigurationRow($row);
    }

    /**
     * @param array<non-empty-string, non-empty-string> $data
     */
    private function setupVaultVariables(array $data, ?string $branchId = null): void
    {
        $attributes = [
            'testId' => 'variables-resolver-test',
        ];

        if ($branchId) {
            $attributes['branchId'] = $branchId;
        }

        $existingVariables = $this->variablesApiClient->listVariables(new ListOptions(attributes: $attributes));
        foreach ($existingVariables as $variable) {
            $this->variablesApiClient->deleteVariable($variable->hash);
        }

        foreach ($data as $key => $value) {
            $this->variablesApiClient->createVariable($key, $value, attributes: $attributes);
        }
    }

    public function testResolveVariables(): void
    {
        $this->setupConfigurationVariables(
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['values' => [['name' => 'foo', 'value' => 'config foo']]]
        );
        $this->setupVaultVariables([
            'foo' => 'vault foo',
        ]);

        $configuration = [
            'variables_id' => self::CONFIG_ID,
            'parameters' => [
                'some_parameter' => 'foo is {{ foo }} and {{ vault.foo }}',
            ],
        ];

        $variableResolver = VariablesResolver::create(
            $this->clientWrapper,
            $this->variablesApiClient,
            $this->logger,
        );

        $newConfiguration = $variableResolver->resolveVariables(
            $configuration,
            $this->mainBranchId,
            self::CONFIG_ROW_ID,
            [],
        );
        self::assertEquals(
            [
                'parameters' => [
                    'some_parameter' => 'foo is config foo and vault foo',
                ],
                'variables_id' => self::CONFIG_ID,
            ],
            $newConfiguration
        );

        self::assertTrue($this->logsHandler->hasInfoThatContains('Replaced values for variables: vault.foo, foo'));
    }

    public function testResolveVariablesInBranch(): void
    {
        $this->setupConfigurationVariables(
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['values' => [['name' => 'foo', 'value' => 'main config foo']]]
        );
        $this->setupVaultVariables([
            'foo' => 'main vault foo',
        ]);

        // prepare dev branch
        $devBranchName = 'variables-resolver-test';
        $masterClientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
            ),
        );
        $branchesApiClient = new DevBranches($masterClientWrapper->getBasicClient());
        foreach ($branchesApiClient->listBranches() as $branch) {
            if ($branch['name'] === $devBranchName) {
                $branchesApiClient->deleteBranch($branch['id']);
                break;
            }
        }
        $devBranch = $branchesApiClient->createBranch($devBranchName);

        $devBranchClientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
                (string) $devBranch['id'],
            ),
        );

        // override config row in dev branch
        $configuration = new StorageConfiguration();
        $configuration->setComponentId(ComponentsClientHelper::KEBOOLA_VARIABLES);
        $configuration->setConfigurationId(self::CONFIG_ID);

        $newRow = new ConfigurationRow($configuration);
        $newRow->setRowId(self::CONFIG_ROW_ID);
        $newRow->setConfiguration(['values' => [['name' => 'foo', 'value' => 'dev config foo']]]);

        $components = new Components($devBranchClientWrapper->getBranchClient());
        $components->updateConfigurationRow($newRow);

        // override vault variable in dev branch
        $this->setupVaultVariables(
            [
                'foo' => 'dev vault foo',
            ],
            (string) $devBranch['id'],
        );

        $configuration = [
            'variables_id' => self::CONFIG_ID,
            'parameters' => [
                'some_parameter' => 'foo is {{ foo }} and {{ vault.foo }}',
            ],
        ];

        $variableResolver = VariablesResolver::create(
            $devBranchClientWrapper,
            $this->variablesApiClient,
            $this->logger,
        );

        $newConfiguration = $variableResolver->resolveVariables(
            $configuration,
            (string) $devBranch['id'], // @phpstan-ignore-line non-empty-string
            self::CONFIG_ROW_ID,
            [],
        );
        self::assertEquals(
            [
                'parameters' => [
                    'some_parameter' => 'foo is dev config foo and dev vault foo',
                ],
                'variables_id' => self::CONFIG_ID,
            ],
            $newConfiguration
        );

        self::assertTrue($this->logsHandler->hasInfoThatContains('Replaced values for variables: vault.foo, foo'));
    }

    public function testMissingVariables(): void
    {
        $this->setupConfigurationVariables(
            ['variables' => []],
            []
        );
        $this->setupVaultVariables([]);

        $configuration = [
            'variables_id' => self::CONFIG_ID,
            'parameters' => [
                'some_parameter' => 'foo is {{ foo }} and {{ vault.foo }}',
            ],
        ];

        $variableResolver = VariablesResolver::create(
            $this->clientWrapper,
            $this->variablesApiClient,
            $this->logger,
        );

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Missing values for placeholders: vault.foo, foo');

        $variableResolver->resolveVariables(
            $configuration,
            $this->mainBranchId,
            self::CONFIG_ROW_ID,
            [],
        );
    }
}
