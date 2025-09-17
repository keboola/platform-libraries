<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Tests;

use Keboola\ConfigurationVariablesResolver\ComponentsClientHelper;
use Keboola\ConfigurationVariablesResolver\Exception\UserException;
use Keboola\ConfigurationVariablesResolver\VariablesResolver;
use Keboola\StorageApi\Client as StorageClient;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\Components\Configuration as StorageConfiguration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\VaultApiClient\Variables\VariablesApiClient;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class VariableResolverTest extends TestCase
{
    private const BRANCH_ID = '123';

    private ClientWrapper $clientWrapper;

    private LoggerInterface $logger;
    private TestHandler $logsHandler;

    public function setUp(): void
    {
        parent::setUp();
        $this->clientWrapper = $this->getClientWrapper();

        $this->logsHandler = new TestHandler();
        $this->logger = new Logger('tests', [$this->logsHandler]);
    }

    private function getClientWrapper(): ClientWrapper
    {
        return new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN'),
            ),
        );
    }

    private function getVariablesResolver(): VariablesResolver
    {
        $variablesApiClient = $this->createMock(VariablesApiClient::class);
        $variablesApiClient
            ->method('listScopedVariablesForBranch')
            ->with(self::BRANCH_ID)
            ->willReturn([])
        ;

        return VariablesResolver::create(
            $this->clientWrapper,
            $variablesApiClient,
            $this->logger,
        );
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
            ['values' => [['name' => 'foo', 'value' => 'bar']]],
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}.'],
        ];
        $variableResolver = $this->getVariablesResolver();
        $resolveResults = $variableResolver->resolveVariables($configuration, self::BRANCH_ID, $vRowId, []);

        self::assertSame(
            [
                'variables_id' => $vConfigurationId,
                'parameters' => [
                    'some_parameter' => 'foo is bar.',
                ],
            ],
            $resolveResults->configuration,
        );
        self::assertSame(
            [
                'foo' => 'bar',
            ],
            $resolveResults->replacedVariablesValues,
        );
        self::assertTrue($this->logsHandler->hasInfoThatContains('Replacing variables using values with ID:'));
        //self::assertTrue($this->logsHandler->hasInfoThatContains('Replaced values for variables: foo'));
    }

    public function testResolveVariablesValuesData(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            [],
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariablesResolver();
        $resolveResults = $variableResolver->resolveVariables(
            $configuration,
            self::BRANCH_ID,
            null,
            ['values' => [['name' => 'foo', 'value' => 'bar']]],
        );
        self::assertSame(
            [
                'variables_id' => $vConfigurationId,
                'parameters' => [
                    'some_parameter' => 'foo is bar',
                ],
            ],
            $resolveResults->configuration,
        );
        self::assertSame(
            [
                'foo' => 'bar',
            ],
            $resolveResults->replacedVariablesValues,
        );
        //self::assertTrue($this->logsHandler->hasInfoThatContains('Replacing variables using inline values.'));
        //self::assertTrue($this->logsHandler->hasInfoThatContains('Replaced values for variables: foo'));
    }

    public function testResolveVariablesDefaultValues(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['values' => [['name' => 'foo', 'value' => 'bar']]],
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'variables_values_id' => $vRowId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariablesResolver();
        $resolveResults = $variableResolver->resolveVariables($configuration, self::BRANCH_ID, null, null);
        self::assertSame(
            [
                'variables_id' => $vConfigurationId,
                'variables_values_id' => $vRowId,
                'parameters' => [
                    'some_parameter' => 'foo is bar',
                ],
            ],
            $resolveResults->configuration,
        );
        self::assertSame(
            [
                'foo' => 'bar',
            ],
            $resolveResults->replacedVariablesValues,
        );
        self::assertTrue($this->logsHandler->hasInfoThatContains('Replacing variables using default values with ID:'));
        //self::assertTrue($this->logsHandler->hasInfoThatContains('Replaced values for variables: foo'));
    }

    public function testResolveVariablesDefaultValuesOverride(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['values' => [['name' => 'foo', 'value' => 'bar']]],
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'variables_values_id' => 'not-used',
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariablesResolver();
        $resolveResults = $variableResolver->resolveVariables($configuration, self::BRANCH_ID, $vRowId, null);
        self::assertSame(
            [
                'variables_id' => $vConfigurationId,
                'variables_values_id' => 'not-used',
                'parameters' => [
                    'some_parameter' => 'foo is bar',
                ],
            ],
            $resolveResults->configuration,
        );
        self::assertSame(
            [
                'foo' => 'bar',
            ],
            $resolveResults->replacedVariablesValues,
        );
        self::assertTrue($this->logsHandler->hasInfoThatContains('Replacing variables using values with ID:'));
        //self::assertTrue($this->logsHandler->hasInfoThatContains('Replaced values for variables: foo'));
    }

    public function testResolveVariablesDefaultValuesOverrideData(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['values' => [['name' => 'foo', 'value' => 'bar']]],
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'variables_values_id' => $vRowId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariablesResolver();
        $resolveResults = $variableResolver->resolveVariables(
            $configuration,
            self::BRANCH_ID,
            null,
            ['values' => [['name' => 'foo', 'value' => 'bazooka']]],
        );
        self::assertSame(
            [
                'variables_id' => $vConfigurationId,
                'variables_values_id' => $vRowId,
                'parameters' => [
                    'some_parameter' => 'foo is bazooka',
                ],
            ],
            $resolveResults->configuration,
        );
        self::assertSame(
            [
                'foo' => 'bazooka',
            ],
            $resolveResults->replacedVariablesValues,
        );
        //self::assertTrue($this->logsHandler->hasInfoThatContains('Replacing variables using inline values.'));
        //self::assertTrue($this->logsHandler->hasInfoThatContains('Replaced values for variables: foo'));
    }

    public function testResolveVariablesNoValues(): void
    {
        list ($vConfigurationId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            [],
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariablesResolver();

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'No variable values provided for variables configuration "' .
            $vConfigurationId . '".',
        );
        $variableResolver->resolveVariables($configuration, self::BRANCH_ID, null, null);
    }

    public function testResolveVariablesInvalidDefaultValues(): void
    {
        list ($vConfigurationId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            [],
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'variables_values_id' => 'non-existent',
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariablesResolver();
        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Cannot read variable values "non-existent" of variables configuration "' . $vConfigurationId .'".',
        );
        $variableResolver->resolveVariables($configuration, self::BRANCH_ID, null, null);
    }

    public function testResolveVariablesInvalidProvidedValues(): void
    {
        list ($vConfigurationId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            [],
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariablesResolver();
        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Cannot read variable values "non-existent" of variables configuration "' . $vConfigurationId .'".',
        );
        $variableResolver->resolveVariables($configuration, self::BRANCH_ID, 'non-existent', null);
    }

    public function testResolveVariablesInvalidProvidedArguments(): void
    {
        list ($vConfigurationId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            [],
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariablesResolver();
        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Only one of variableValuesId and variableValuesData can be entered.',
        );
        $variableResolver->resolveVariables(
            $configuration,
            self::BRANCH_ID,
            'non-existent',
            ['values' => [['name' => 'foo', 'value' => 'bar']]],
        );
    }

    public function testResolveVariablesWithEmptyValuesArray(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['values' => [['name' => 'foo', 'value' => 'bar']]],
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariablesResolver();
        $resolveResults = $variableResolver->resolveVariables(
            $configuration,
            self::BRANCH_ID,
            $vRowId,
            ['values' => []],
        );
        self::assertSame(
            [
                'variables_id' => $vConfigurationId,
                'parameters' => [
                    'some_parameter' => 'foo is bar',
                ],
            ],
            $resolveResults->configuration,
        );
        self::assertSame(
            [
                'foo' => 'bar',
            ],
            $resolveResults->replacedVariablesValues,
        );
        self::assertTrue($this->logsHandler->hasInfoThatContains('Replacing variables using values with ID:'));
        //self::assertTrue($this->logsHandler->hasInfoThatContains('Replaced values for variables: foo'));
    }

    public function testResolveVariablesNonExistentVariableConfiguration(): void
    {
        $configuration = [
            'variables_id' => 'non-existent',
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariablesResolver();
        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Variable configuration cannot be read: Configuration "non-existent" not found',
        );
        $variableResolver->resolveVariables($configuration, self::BRANCH_ID, 'non-existent', null);
    }

    public function testResolveVariablesInvalidVariableConfiguration(): void
    {
        list ($vConfigurationId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['invalid' => 'data'],
            [],
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariablesResolver();
        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Variable configuration is invalid: Unrecognized option "invalid" under "configuration". ' .
            'Available option is "variables".',
        );
        $variableResolver->resolveVariables($configuration, self::BRANCH_ID, 'non-existent', null);
    }

    public function testResolveVariablesNoVariables(): void
    {
        $configuration = [
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariablesResolver();
        $resolveResults = $variableResolver->resolveVariables($configuration, self::BRANCH_ID, '123', []);
        self::assertSame(
            [
                'parameters' => [
                    'some_parameter' => 'foo is {{ foo }}',
                ],
            ],
            $resolveResults->configuration,
        );
        self::assertSame([], $resolveResults->replacedVariablesValues);
        self::assertFalse($this->logsHandler->hasInfoThatContains('Replacing variables using default values with ID:'));
    }

    public function testInvalidValuesConfiguration(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['invalid' => [['name' => 'foo', 'value' => 'bar']]],
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }} and {{ notreplaced }}.'],
        ];
        $variableResolver = $this->getVariablesResolver();
        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Variable values configuration is invalid: Unrecognized option "invalid" under "configuration". ' .
            'Available option is "values".',
        );
        $variableResolver->resolveVariables($configuration, self::BRANCH_ID, $vRowId, []);
    }

    public function testResolveVariablesSpecialCharacterReplacement(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            [],
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}'],
        ];
        $variableResolver = $this->getVariablesResolver();
        $resolveResults = $variableResolver->resolveVariables(
            $configuration,
            self::BRANCH_ID,
            null,
            ['values' => [['name' => 'foo', 'value' => 'special " \' { } characters']]],
        );
        self::assertEquals(
            [
                'parameters' => [
                    'some_parameter' => 'foo is special " \' { } characters',
                ],
                'variables_id' => $vConfigurationId,
            ],
            $resolveResults->configuration,
        );
        self::assertSame(
            [
                'foo' => 'special " \' { } characters',
            ],
            $resolveResults->replacedVariablesValues,
        );
        //self::assertTrue($this->logsHandler->hasInfoThatContains('Replacing variables using inline values.'));
        //self::assertTrue($this->logsHandler->hasInfoThatContains('Replaced values for variables: foo'));
    }

    public function testResolveVariablesMissingValues(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string'], ['name' => 'goo', 'type' => 'string']]],
            [],
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }}.'],
        ];
        $variableResolver = $this->getVariablesResolver();
        $this->expectException(UserException::class);
        $this->expectExceptionMessage('No value provided for variable "goo".');
        $variableResolver->resolveVariables(
            $configuration,
            self::BRANCH_ID,
            null,
            ['values' => [['name' => 'foo', 'value' => 'bar']]],
        );
    }

    public function testResolveVariablesMissingValuesInBody(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            [],
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ foo }} and bar is {{ bar }} and baz is {{ baz }}.'],
        ];
        $variableResolver = $this->getVariablesResolver();
        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Missing values for placeholders: bar, baz');
        $variableResolver->resolveVariables(
            $configuration,
            self::BRANCH_ID,
            null,
            ['values' => [['name' => 'foo', 'value' => 'bar']]],
        );
    }

    public function testResolveVariablesValuesBranch(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 'foo', 'type' => 'string']]],
            ['values' => [['name' => 'foo', 'value' => 'bar']]],
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
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
                (string) $branchId,
            ),
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
        $variableResolver = $this->getVariablesResolver();
        $resolveResults = $variableResolver->resolveVariables($configuration, self::BRANCH_ID, $vRowId, []);
        self::assertSame(
            [
                'variables_id' => $vConfigurationId,
                'parameters' => [
                    'some_parameter' => 'foo is dev-bar.',
                ],
            ],
            $resolveResults->configuration,
        );
        self::assertSame(
            [
                'foo' => 'dev-bar',
            ],
            $resolveResults->replacedVariablesValues,
        );
        self::assertTrue($this->logsHandler->hasInfoThatContains('Replacing variables using values with ID:'));
        //self::assertTrue($this->logsHandler->hasInfoThatContains('Replaced values for variables: foo'));
    }

    public function testResolveVariablesIntegerNameAndValue(): void
    {
        list ($vConfigurationId, $vRowId) = $this->createVariablesConfiguration(
            $this->clientWrapper->getBasicClient(),
            ['variables' => [['name' => 4321, 'type' => 'string']]],
            [],
        );
        $configuration = [
            'variables_id' => $vConfigurationId,
            'parameters' => ['some_parameter' => 'foo is {{ 4321 }}'],
        ];
        $variableResolver = $this->getVariablesResolver();
        $resolveResults = $variableResolver->resolveVariables(
            $configuration,
            self::BRANCH_ID,
            null,
            ['values' => [['name' => 4321, 'value' => 1234]]],
        );
        self::assertSame(
            [
                'variables_id' => $vConfigurationId,
                'parameters' => [
                    'some_parameter' => 'foo is 1234',
                ],
            ],
            $resolveResults->configuration,
        );
        self::assertSame(
            [
                0 => '1234',
            ],
            $resolveResults->replacedVariablesValues,
        );
        //self::assertTrue($this->logsHandler->hasInfoThatContains('Replacing variables using inline values.'));
        //self::assertTrue($this->logsHandler->hasInfoThatContains('Replaced values for variables: 4321'));
    }
}
