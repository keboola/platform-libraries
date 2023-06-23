<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Tests;

use Keboola\ConfigurationVariablesResolver\ComponentsClientHelper;
use Keboola\ConfigurationVariablesResolver\Exception\UserException;
use Keboola\ConfigurationVariablesResolver\VariableResolver;
use Keboola\StorageApi\Client as StorageClient;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\Components\Configuration as StorageConfiguration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Psr\Log\Test\TestLogger;

class VariableResolverTest extends TestCase
{
    private ClientWrapper $clientWrapper;

    private TestLogger $testLogger;

    public function setUp(): void
    {
        parent::setUp();
        $this->clientWrapper = $this->getClientWrapper();
        $this->testLogger = new TestLogger();
    }

    private function getClientWrapper(): ClientWrapper
    {
        return new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN'),
            )
        );
    }

    private function getVariableResolver(): VariableResolver
    {
        return VariableResolver::create($this->clientWrapper, $this->testLogger);
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

    public function testResolveVariablesValuesId(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['values' => [['name' => 'foo', 'value' => 'bar']]]
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}.'],
        ];
        $variableResolver = $this->getVariableResolver();
        $newConfiguration = $variableResolver->resolveVariables($configuration, $vRowId, []);

        self::assertEquals(
            [
                'parameters' => [
                    'some_parameter' => 'foo is bar.',
                ],
                'variables_id' => $vConfigurationId,
            ],
            $newConfiguration
        );
        self::assertTrue($this->testLogger->hasInfoThatContains('Replacing variables using values with ID:'));
        self::assertTrue($this->testLogger->hasInfoThatContains('Replaced values for variables: foo.'));
    }

    public function testResolveVariablesValuesData(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            []
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariableResolver();
        $newConfiguration = $variableResolver->resolveVariables(
            $configuration,
            null,
            ['values' => [['name' => 'foo', 'value' => 'bar']]]
        );
        self::assertEquals(
            [
                'parameters' => [
                    'some_parameter' => 'foo is bar',
                ],
                'variables_id' => $vConfigurationId,
            ],
            $newConfiguration
        );
        self::assertTrue($this->testLogger->hasInfoThatContains('Replacing variables using inline values.'));
        self::assertTrue($this->testLogger->hasInfoThatContains('Replaced values for variables: foo.'));
    }

    public function testResolveVariablesDefaultValues(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['values' => [['name' => 'foo', 'value' => 'bar']]]
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'variables_values_id' => $vRowId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariableResolver();
        $newConfiguration = $variableResolver->resolveVariables($configuration, null, null);
        self::assertEquals(
            [
                'parameters' => [
                    'some_parameter' => 'foo is bar',
                ],
                'variables_id' => $vConfigurationId,
                'variables_values_id' => $vRowId,
            ],
            $newConfiguration
        );
        self::assertTrue($this->testLogger->hasInfoThatContains('Replacing variables using default values with ID:'));
        self::assertTrue($this->testLogger->hasInfoThatContains('Replaced values for variables: foo.'));
    }

    public function testResolveVariablesDefaultValuesOverride(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['values' => [['name' => 'foo', 'value' => 'bar']]]
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'variables_values_id' => 'not-used',
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariableResolver();
        $newConfiguration = $variableResolver->resolveVariables($configuration, $vRowId, null);
        self::assertEquals(
            [
                'parameters' => [
                    'some_parameter' => 'foo is bar',
                ],
                'variables_id' => $vConfigurationId,
                'variables_values_id' => 'not-used',
            ],
            $newConfiguration
        );
        self::assertTrue($this->testLogger->hasInfoThatContains('Replacing variables using values with ID:'));
        self::assertTrue($this->testLogger->hasInfoThatContains('Replaced values for variables: foo.'));
    }

    public function testResolveVariablesDefaultValuesOverrideData(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['values' => [['name' => 'foo', 'value' => 'bar']]]
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'variables_values_id' => $vRowId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariableResolver();
        $newConfiguration = $variableResolver->resolveVariables(
            $configuration,
            null,
            ['values' => [['name' => 'foo', 'value' => 'bazooka']]]
        );
        self::assertEquals(
            [
                'parameters' => [
                    'some_parameter' => 'foo is bazooka',
                ],
                'variables_id' => $vConfigurationId,
                'variables_values_id' => $vRowId,
            ],
            $newConfiguration
        );
        self::assertTrue($this->testLogger->hasInfoThatContains('Replacing variables using inline values.'));
        self::assertTrue($this->testLogger->hasInfoThatContains('Replaced values for variables: foo.'));
    }

    public function testResolveVariablesNoValues(): void
    {
        list ($vConfigurationId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            []
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariableResolver();

        self::expectException(UserException::class);
        self::expectExceptionMessage(
            'No variable values provided for variables configuration "' .
            $vConfigurationId . '".'
        );
        $variableResolver->resolveVariables($configuration, null, null);
    }

    public function testResolveVariablesInvalidDefaultValues(): void
    {
        list ($vConfigurationId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            []
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'variables_values_id' => 'non-existent',
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariableResolver();
        self::expectException(UserException::class);
        self::expectExceptionMessage(
            'Cannot read variable values "non-existent" of variables configuration "' . $vConfigurationId .'".'
        );
        $variableResolver->resolveVariables($configuration, null, null);
    }

    public function testResolveVariablesInvalidProvidedValues(): void
    {
        list ($vConfigurationId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            []
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariableResolver();
        self::expectException(UserException::class);
        self::expectExceptionMessage(
            'Cannot read variable values "non-existent" of variables configuration "' . $vConfigurationId .'".'
        );
        $variableResolver->resolveVariables($configuration, 'non-existent', null);
    }

    public function testResolveVariablesInvalidProvidedArguments(): void
    {
        list ($vConfigurationId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            []
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = VariableResolver::create($this->clientWrapper, $this->testLogger);
        self::expectException(UserException::class);
        self::expectExceptionMessage(
            'Only one of variableValuesId and variableValuesData can be entered.'
        );
        $variableResolver->resolveVariables(
            $configuration,
            'non-existent',
            ['values' => [['name' => 'foo', 'value' => 'bar']]]
        );
    }

    public function testResolveVariablesWithEmptyValuesArray(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['values' => [['name' => 'foo', 'value' => 'bar']]]
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariableResolver();
        $newConfiguration = $variableResolver->resolveVariables(
            $configuration,
            $vRowId,
            ['values' => []]
        );
        self::assertEquals(
            [
                'parameters' => [
                    'some_parameter' => 'foo is bar',
                ],
                'variables_id' => $vConfigurationId,
            ],
            $newConfiguration
        );
        self::assertTrue($this->testLogger->hasInfoThatContains('Replacing variables using values with ID:'));
        self::assertTrue($this->testLogger->hasInfoThatContains('Replaced values for variables: foo.'));
    }

    public function testResolveVariablesNonExistentVariableConfiguration(): void
    {
        $configuration = [
            'variables_id' => 'non-existent',
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariableResolver();
        self::expectException(UserException::class);
        self::expectExceptionMessage(
            'Variable configuration cannot be read: Configuration non-existent not found'
        );
        $variableResolver->resolveVariables($configuration, 'non-existent', null);
    }

    public function testResolveVariablesInvalidVariableConfiguration(): void
    {
        list ($vConfigurationId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['invalid' => 'data'],
            []
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariableResolver();
        self::expectException(UserException::class);
        self::expectExceptionMessage(
            'Variable configuration is invalid: Unrecognized option "invalid" under "variables". ' .
            'Available option is "variables".'
        );
        $variableResolver->resolveVariables($configuration, 'non-existent', null);
    }

    public function testResolveVariablesNoVariables(): void
    {
        $configuration = [
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariableResolver();

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Missing values for placeholders: foo');
        $variableResolver->resolveVariables($configuration, '123', []);
    }

    public function testInvalidValuesConfiguration(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['invalid' => [['name' => 'foo', 'value' => 'bar']]]
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }} and {{ notreplaced }}.'],
        ];
        $variableResolver = $this->getVariableResolver();
        self::expectException(UserException::class);
        self::expectExceptionMessage(
            'Variable values configuration is invalid: Unrecognized option "invalid" under "values". ' .
            'Available option is "values".'
        );
        $variableResolver->resolveVariables($configuration, $vRowId, []);
    }

    public function testResolveVariablesSpecialCharacterReplacement(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            []
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariableResolver();
        $newConfiguration = $variableResolver->resolveVariables(
            $configuration,
            null,
            ['values' => [['name' => 'foo', 'value' => 'special " \' { } characters']]]
        );
        self::assertEquals(
            [
                'parameters' => [
                    'some_parameter' => 'foo is special " \' { } characters',
                ],
                'variables_id' => $vConfigurationId,
            ],
            $newConfiguration
        );
        self::assertTrue($this->testLogger->hasInfoThatContains('Replacing variables using inline values.'));
        self::assertTrue($this->testLogger->hasInfoThatContains('Replaced values for variables: foo.'));
    }

    public function testResolveVariablesSpecialCharacterNonEscapedReplacement(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            []
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{{ foo }}}'],
        ];
        $variableResolver = $this->getVariableResolver();
        self::expectException(UserException::class);
        self::expectExceptionMessage('Variable replacement resulted in invalid configuration, error: Syntax error');
        $variableResolver->resolveVariables(
            $configuration,
            null,
            ['values' => [['name' => 'foo', 'value' => 'special " \' { } characters']]]
        );
    }

    public function testResolveVariablesMissingValues(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string'], ['name' => 'goo', 'type' => 'string']]],
            []
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}.'],
        ];
        $variableResolver = $this->getVariableResolver();
        self::expectException(UserException::class);
        self::expectExceptionMessage('No value provided for variable "goo".');
        $variableResolver->resolveVariables(
            $configuration,
            null,
            ['values' => [['name' => 'foo', 'value' => 'bar']]]
        );
    }

    public function testResolveVariablesMissingValuesInBody(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            []
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }} and bar is {{ bar }} and baz is {{ baz }}.'],
        ];
        $variableResolver = $this->getVariableResolver();
        self::expectException(UserException::class);
        self::expectExceptionMessage('Missing values for placeholders: bar, baz');
        $variableResolver->resolveVariables(
            $configuration,
            null,
            ['values' => [['name' => 'foo', 'value' => 'bar']]]
        );
    }

    public function testResolveVariablesValuesBranch(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
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
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
                (string) $branchId
            )
        );

        // modify the dev branch variable configuration to "dev-bar"
        $components = new Components($this->clientWrapper->getBranchClient());
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
        $variableResolver = $this->getVariableResolver();
        $newConfiguration = $variableResolver->resolveVariables($configuration, $vRowId, []);
        self::assertEquals(
            [
                'parameters' => [
                    'some_parameter' => 'foo is dev-bar.',
                ],
                'variables_id' => $vConfigurationId,
            ],
            $newConfiguration
        );
        self::assertTrue($this->testLogger->hasInfoThatContains('Replacing variables using values with ID:'));
        self::assertTrue($this->testLogger->hasInfoThatContains('Replaced values for variables: foo.'));
    }

    public function testResolveVariablesIntegerNameAndValue(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 4321, 'type' => 'string']]],
            []
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ 4321 }}'],
        ];
        $variableResolver = $this->getVariableResolver();
        $newConfiguration = $variableResolver->resolveVariables(
            $configuration,
            null,
            ['values' => [['name' => 4321, 'value' => 1234]]]
        );
        self::assertEquals(
            [
                'parameters' => [
                    'some_parameter' => 'foo is 1234',
                ],
                'variables_id' => $vConfigurationId,
            ],
            $newConfiguration
        );
        self::assertTrue($this->testLogger->hasInfoThatContains('Replacing variables using inline values.'));
        self::assertTrue($this->testLogger->hasInfoThatContains('Replaced values for variables: 4321.'));
    }
}
