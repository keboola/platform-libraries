<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Tests\VariablesLoader;

use Keboola\ConfigurationVariablesResolver\ComponentsClientHelper;
use Keboola\ConfigurationVariablesResolver\Exception\UserException;
use Keboola\ConfigurationVariablesResolver\VariablesLoader\ConfigurationVariablesLoader;
use Keboola\StorageApi\Client as StorageClient;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\Components\Configuration as StorageConfiguration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class ConfigurationVariablesLoaderTest extends TestCase
{
    private readonly ClientWrapper $clientWrapper;
    private readonly TestHandler $logsHandler;
    private readonly LoggerInterface $logger;

    public function setUp(): void
    {
        parent::setUp();

        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN'),
            )
        );
        $this->logsHandler = new TestHandler();
        $this->logger = new Logger('tests', [$this->logsHandler]);
    }

    private function createVariablesConfiguration(StorageClient $storageClient, array $data, array $rowData): array
    {
        $components = new Components($storageClient);
        $configuration = new StorageConfiguration();
        $configuration->setComponentId(ComponentsClientHelper::KEBOOLA_VARIABLES);
        $configuration->setName('variables-resolver-test');
        $configuration->setConfiguration($data);
        $configId = $components->addConfiguration($configuration)['id'];
        $configuration->setConfigurationId($configId);
        $row = new ConfigurationRow($configuration);
        $row->setName('variables-resolver-test-row');
        $row->setConfiguration($rowData);
        $rowId = $components->addConfigurationRow($row)['id'];

        return [$configId, $rowId];
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

    public function testLoadVariablesValuesId(): void
    {
        [$vConfigurationId, $vRowId] = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['values' => [['name' => 'foo', 'value' => 'bar']]]
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}.'],
        ];

        $variablesLoader = new ConfigurationVariablesLoader(
            new ComponentsClientHelper($this->clientWrapper),
            $this->logger,
        );
        $variables = $variablesLoader->loadVariables($configuration, $vRowId, []);

        self::assertSame(['foo' => 'bar'], $variables);
        self::assertTrue($this->logsHandler->hasInfoThatContains('Replacing variables using values with ID:'));
    }

    public function testLoadVariablesValuesData(): void
    {
        [$vConfigurationId] = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            []
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];

        $variablesLoader = new ConfigurationVariablesLoader(
            new ComponentsClientHelper($this->clientWrapper),
            $this->logger,
        );
        $variables = $variablesLoader->loadVariables(
            $configuration,
            null,
            ['values' => [['name' => 'foo', 'value' => 'bar']]]
        );

        self::assertSame(['foo' => 'bar'], $variables);
        self::assertTrue($this->logsHandler->hasInfoThatContains('Replacing variables using inline values.'));
    }

    public function testLoadVariablesDefaultValues(): void
    {
        [$vConfigurationId, $vRowId] = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['values' => [['name' => 'foo', 'value' => 'bar']]]
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'variables_values_id' => $vRowId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];

        $variablesLoader = new ConfigurationVariablesLoader(
            new ComponentsClientHelper($this->clientWrapper),
            $this->logger,
        );
        $variables = $variablesLoader->loadVariables($configuration, null, null);

        self::assertSame(['foo' => 'bar'], $variables);
        self::assertTrue($this->logsHandler->hasInfoThatContains('Replacing variables using default values with ID:'));
    }

    public function testLoadVariablesDefaultValuesOverride(): void
    {
        [$vConfigurationId, $vRowId] = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['values' => [['name' => 'foo', 'value' => 'bar']]]
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'variables_values_id' => 'not-used',
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];

        $variablesLoader = new ConfigurationVariablesLoader(
            new ComponentsClientHelper($this->clientWrapper),
            $this->logger,
        );
        $variables = $variablesLoader->loadVariables($configuration, $vRowId, null);

        self::assertSame(['foo' => 'bar'], $variables);
        self::assertTrue($this->logsHandler->hasInfoThatContains('Replacing variables using values with ID:'));
    }

    public function testLoadVariablesDefaultValuesOverrideData(): void
    {
        [$vConfigurationId, $vRowId] = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['values' => [['name' => 'foo', 'value' => 'bar']]]
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'variables_values_id' => $vRowId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];

        $variablesLoader = new ConfigurationVariablesLoader(
            new ComponentsClientHelper($this->clientWrapper),
            $this->logger,
        );
        $variables = $variablesLoader->loadVariables(
            $configuration,
            null,
            ['values' => [['name' => 'foo', 'value' => 'bazooka']]],
        );

        self::assertSame(['foo' => 'bazooka'], $variables);
        self::assertTrue($this->logsHandler->hasInfoThatContains('Replacing variables using inline values.'));
    }

    public function testLoadVariablesNoValues(): void
    {
        [$vConfigurationId] = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            []
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];

        $variablesLoader = new ConfigurationVariablesLoader(
            new ComponentsClientHelper($this->clientWrapper),
            $this->logger,
        );

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'No variable values provided for variables configuration "' .
            $vConfigurationId . '".'
        );

        $variablesLoader->loadVariables($configuration, null, null);
    }

    public function testLoadVariablesInvalidDefaultValues(): void
    {
        [$vConfigurationId] = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            []
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'variables_values_id' => 'non-existent',
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];

        $variablesLoader = new ConfigurationVariablesLoader(
            new ComponentsClientHelper($this->clientWrapper),
            $this->logger,
        );

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Cannot read variable values "non-existent" of variables configuration "' . $vConfigurationId .'".'
        );
        $variablesLoader->loadVariables($configuration, null, null);
    }

    public function testLoadVariablesInvalidProvidedValues(): void
    {
        [$vConfigurationId] = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            []
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];

        $variablesLoader = new ConfigurationVariablesLoader(
            new ComponentsClientHelper($this->clientWrapper),
            $this->logger,
        );

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Cannot read variable values "non-existent" of variables configuration "' . $vConfigurationId .'".'
        );
        $variablesLoader->loadVariables($configuration, 'non-existent', null);
    }

    public function testLoadVariablesInvalidProvidedArguments(): void
    {
        [$vConfigurationId] = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            []
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];

        $variablesLoader = new ConfigurationVariablesLoader(
            new ComponentsClientHelper($this->clientWrapper),
            $this->logger,
        );

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Only one of variableValuesId and variableValuesData can be entered.'
        );

        $variablesLoader->loadVariables(
            $configuration,
            'non-existent',
            ['values' => [['name' => 'foo', 'value' => 'bar']]]
        );
    }

    public function testLoadVariablesWithEmptyValuesArray(): void
    {
        [$vConfigurationId, $vRowId] = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['values' => [['name' => 'foo', 'value' => 'bar']]]
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];

        $variablesLoader = new ConfigurationVariablesLoader(
            new ComponentsClientHelper($this->clientWrapper),
            $this->logger,
        );
        $variables = $variablesLoader->loadVariables(
            $configuration,
            $vRowId,
            ['values' => []]
        );

        self::assertSame(['foo' => 'bar'], $variables);

        self::assertTrue($this->logsHandler->hasInfoThatContains('Replacing variables using values with ID:'));
    }

    public function testLoadVariablesNonExistentVariableConfiguration(): void
    {
        $configuration = [
            'variables_id' => 'non-existent',
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];

        $variablesLoader = new ConfigurationVariablesLoader(
            new ComponentsClientHelper($this->clientWrapper),
            $this->logger,
        );

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Variable configuration cannot be read: Configuration non-existent not found'
        );

        $variablesLoader->loadVariables($configuration, 'non-existent', null);
    }

    public function testLoadVariablesInvalidVariableConfiguration(): void
    {
        [$vConfigurationId] = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['invalid' => 'data'],
            []
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];

        $variablesLoader = new ConfigurationVariablesLoader(
            new ComponentsClientHelper($this->clientWrapper),
            $this->logger,
        );

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Variable configuration is invalid: Unrecognized option "invalid" under "variables". ' .
            'Available option is "variables".'
        );

        $variablesLoader->loadVariables($configuration, 'non-existent', null);
    }

    public function testInvalidValuesConfiguration(): void
    {
        [$vConfigurationId, $vRowId] = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['invalid' => [['name' => 'foo', 'value' => 'bar']]]
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }} and {{ notreplaced }}.'],
        ];

        $variablesLoader = new ConfigurationVariablesLoader(
            new ComponentsClientHelper($this->clientWrapper),
            $this->logger,
        );

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Variable values configuration is invalid: Unrecognized option "invalid" under "values". ' .
            'Available option is "values".'
        );

        $variablesLoader->loadVariables($configuration, $vRowId, []);
    }

    public function testLoadVariablesMissingValues(): void
    {
        [$vConfigurationId] = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string'], ['name' => 'goo', 'type' => 'string']]],
            []
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}.'],
        ];

        $variablesLoader = new ConfigurationVariablesLoader(
            new ComponentsClientHelper($this->clientWrapper),
            $this->logger,
        );
        $this->expectException(UserException::class);
        $this->expectExceptionMessage('No value provided for variable "goo".');

        $variablesLoader->loadVariables(
            $configuration,
            null,
            ['values' => [['name' => 'foo', 'value' => 'bar']]]
        );
    }

    public function testLoadVariablesValuesBranch(): void
    {
        [$vConfigurationId, $vRowId] = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['values' => [['name' => 'foo', 'value' => 'bar']]]
        );
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
            )
        );

        $branchId = $this->createBranch('my-dev-branch', $clientWrapper);
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
                (string) $branchId
            )
        );

        // modify the dev branch variable configuration to "dev-bar"
        $components = new Components($clientWrapper->getBranchClient());
        $configuration = new StorageConfiguration();
        $configuration->setComponentId('keboola.variables');
        $configuration->setConfigurationId($vConfigurationId);
        $newRow = new ConfigurationRow($configuration);
        $newRow->setRowId($vRowId);
        $newRow->setConfiguration(['values' => [['name' => 'foo', 'value' => 'dev-bar']]]);
        $components->updateConfigurationRow($newRow);

        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}.'],
        ];

        $variablesLoader = new ConfigurationVariablesLoader(
            new ComponentsClientHelper($clientWrapper),
            $this->logger,
        );
        $variables = $variablesLoader->loadVariables($configuration, $vRowId, []);

        self::assertSame(['foo' => 'dev-bar'], $variables);
        self::assertTrue($this->logsHandler->hasInfoThatContains('Replacing variables using values with ID:'));
    }

    public function testLoadVariablesIntegerNameAndValue(): void
    {
        [$vConfigurationId] = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 4321, 'type' => 'string']]],
            []
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ 4321 }}'],
        ];

        $variablesLoader = new ConfigurationVariablesLoader(
            new ComponentsClientHelper($this->clientWrapper),
            $this->logger,
        );
        $variables = $variablesLoader->loadVariables(
            $configuration,
            null,
            ['values' => [['name' => 4321, 'value' => 1234]]]
        );

        self::assertSame(['4321' => '1234'], $variables); // @phpstan-ignore-line
        self::assertTrue($this->logsHandler->hasInfoThatContains('Replacing variables using inline values.'));
    }
}
