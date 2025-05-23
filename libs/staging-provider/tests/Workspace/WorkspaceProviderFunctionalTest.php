<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Workspace;

use Keboola\KeyGenerator\PemKeyCertificateGenerator;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StagingProvider\Tests\TestEnvVarsTrait;
use Keboola\StagingProvider\Workspace\Configuration\NetworkPolicy;
use Keboola\StagingProvider\Workspace\Configuration\NewWorkspaceConfig;
use Keboola\StagingProvider\Workspace\SnowflakeKeypairGenerator;
use Keboola\StagingProvider\Workspace\WorkspaceProvider;
use Keboola\StagingProvider\Workspace\WorkspaceWithCredentialsInterface;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\StorageApiToken;
use PHPUnit\Framework\TestCase;
use Throwable;

class WorkspaceProviderFunctionalTest extends TestCase
{
    use TestEnvVarsTrait;

    private readonly Workspaces $workspacesApiClient;
    private readonly Components $componentsApiClient;
    private readonly StorageApiToken $storageApiToken;
    private readonly WorkspaceProvider $workspaceProvider;

    private array $createdWorkspaceIds = [];
    /** @var Configuration[] */
    private array $createdConfigurations = [];

    protected function setUp(): void
    {
        parent::setUp();

        $clientWrapper = new ClientWrapper(new ClientOptions(
            url: self::getRequiredEnv('STORAGE_API_URL'),
            token: self::getRequiredEnv('STORAGE_API_TOKEN'),
        ));

        $this->workspacesApiClient = new Workspaces($clientWrapper->getBranchClient());
        $this->componentsApiClient = new Components($clientWrapper->getBranchClient());
        $this->storageApiToken = $clientWrapper->getToken();

        $this->workspaceProvider = new WorkspaceProvider(
            $this->workspacesApiClient,
            $this->componentsApiClient,
            new SnowflakeKeypairGenerator(new PemKeyCertificateGenerator()),
        );
    }

    protected function tearDown(): void
    {
        foreach ($this->createdWorkspaceIds as $workspaceId) {
            try {
                $this->workspacesApiClient->deleteWorkspace((int) $workspaceId, async: true);
            } catch (ClientException $e) {
                if ($e->getCode() !== 404) {
                    throw $e;
                }
            }
        }

        foreach ($this->createdConfigurations as $configuration) {
            try {
                $this->componentsApiClient->deleteConfiguration(
                    $configuration->getComponentId(),
                    $configuration->getConfigurationId(),
                );
            } catch (ClientException $e) {
                if ($e->getCode() !== 404) {
                    throw $e;
                }
            }
        }

        parent::tearDown();
    }

    private function createWorkspace(array $options): array
    {
        $workspaceData = $this->workspacesApiClient->createWorkspace($options, async: true);
        $this->createdWorkspaceIds[] = (string) $workspaceData['id'];
        return $workspaceData;
    }

    private function createConfiguration(): Configuration
    {
        $componentId = 'keboola.runner-staging-test';
        $configId = 'staging-provider-workspace-provider-functional-test';

        $configuration = new Configuration();
        $configuration->setConfigurationId($configId);
        $configuration->setName('Staging Provider Functional Test');
        $configuration->setComponentId($componentId);
        $this->componentsApiClient->addConfiguration($configuration);

        $this->createdConfigurations[] = $configuration;

        return $configuration;
    }

    public function testCreateNewWorkspaceWithoutConfigId(): void
    {
        $config = new NewWorkspaceConfig(
            stagingType: StagingType::WorkspaceSnowflake,
            componentId: 'keboola.staging-provider-functional-test',
            configId: null,
            size: 'small',
            useReadonlyRole: null,
            networkPolicy: NetworkPolicy::USER,
            loginType: null,
        );

        $workspace = $this->workspaceProvider->createNewWorkspace(
            $this->storageApiToken,
            $config,
        );
        $this->createdWorkspaceIds[] = $workspace->getWorkspaceId();

        self::assertNotEmpty($workspace->getWorkspaceId());
        self::assertSame('snowflake', $workspace->getBackendType());
        self::assertSame('small', $workspace->getBackendSize());
        self::assertNotEmpty($workspace->getCredentials()['host'] ?? null);
        self::assertNotEmpty($workspace->getCredentials()['user'] ?? null);
        self::assertNotEmpty($workspace->getCredentials()['password'] ?? null);
        self::assertNotEmpty($workspace->getCredentials()['database'] ?? null);
        self::assertNotEmpty($workspace->getCredentials()['schema'] ?? null);
        self::assertNotEmpty($workspace->getCredentials()['warehouse'] ?? null);
    }

    public function testCreateNewWorkspaceWithConfigId(): void
    {
        $configuration = $this->createConfiguration();

        $config = new NewWorkspaceConfig(
            StagingType::WorkspaceSnowflake,
            $configuration->getComponentId(),     // @phpstan-ignore-line getComponentId return type is set to mixed
            $configuration->getConfigurationId(), // @phpstan-ignore-line getConfigurationId return type is set to mixed
            'small',
            null,
            NetworkPolicy::USER,
            null,
        );

        $workspace = $this->workspaceProvider->createNewWorkspace(
            $this->storageApiToken,
            $config,
        );
        $this->createdWorkspaceIds[] = $workspace->getWorkspaceId();

        self::assertNotEmpty($workspace->getWorkspaceId());
        self::assertSame('snowflake', $workspace->getBackendType());
        self::assertSame('small', $workspace->getBackendSize());
        self::assertNotEmpty($workspace->getCredentials()['host'] ?? null);
        self::assertNotEmpty($workspace->getCredentials()['user'] ?? null);
        self::assertNotEmpty($workspace->getCredentials()['password'] ?? null);
        self::assertNotEmpty($workspace->getCredentials()['database'] ?? null);
        self::assertNotEmpty($workspace->getCredentials()['schema'] ?? null);
        self::assertNotEmpty($workspace->getCredentials()['warehouse'] ?? null);

        $workspaceData = $this->workspacesApiClient->getWorkspace($workspace->getWorkspaceId());
        self::assertSame($configuration->getConfigurationId(), $workspaceData['configurationId']);
        self::assertSame($configuration->getComponentId(), $workspaceData['component']);
    }

    public function testCreateNewSnowflakeWorkspaceWithKeyPairAuth(): void
    {
        $config = new NewWorkspaceConfig(
            StagingType::WorkspaceSnowflake,
            'keboola.staging-provider-functional-test',
            null,
            'small',
            null,
            NetworkPolicy::USER,
            WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
        );

        $workspace = $this->workspaceProvider->createNewWorkspace(
            $this->storageApiToken,
            $config,
        );
        $this->createdWorkspaceIds[] = $workspace->getWorkspaceId();

        self::assertNotEmpty($workspace->getWorkspaceId());
        self::assertSame('snowflake', $workspace->getBackendType());
        self::assertSame('small', $workspace->getBackendSize());
        self::assertSame(WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR, $workspace->getLoginType());
        self::assertNotEmpty($workspace->getCredentials()['host'] ?? null);
        self::assertNotEmpty($workspace->getCredentials()['user'] ?? null);
        self::assertNull($workspace->getCredentials()['password'] ?? null);
        self::assertNotEmpty($workspace->getCredentials()['privateKey'] ?? null);
        self::assertStringStartsWith('-----BEGIN PRIVATE KEY-----', $workspace->getCredentials()['privateKey']);
        self::assertNotEmpty($workspace->getCredentials()['database'] ?? null);
        self::assertNotEmpty($workspace->getCredentials()['schema'] ?? null);
        self::assertNotEmpty($workspace->getCredentials()['warehouse'] ?? null);
    }

    public function testGetExistingWorkspaceWithoutCredentials(): void
    {
        $workspaceData = $this->createWorkspace([
            'backend' => 'snowflake',
            'networkPolicy' => 'system',
        ]);
        $workspaceId = (string) $workspaceData['id'];

        $workspace = $this->workspaceProvider->getExistingWorkspace($workspaceId, null);

        self::assertNotInstanceOf(WorkspaceWithCredentialsInterface::class, $workspace);
        self::assertSame((string) $workspaceData['id'], $workspace->getWorkspaceId());
        self::assertSame($workspaceData['connection']['backend'], $workspace->getBackendType());
        self::assertSame($workspaceData['backendSize'], $workspace->getBackendSize());
        self::assertSame($workspaceData['connection']['loginType'], $workspace->getLoginType()->value);
    }

    public function testGetExistingWorkspaceWithCredentials(): void
    {
        $workspaceData = $this->createWorkspace([
            'backend' => 'snowflake',
            'networkPolicy' => 'system',
        ]);
        $workspaceId = (string) $workspaceData['id'];

        $workspace = $this->workspaceProvider->getExistingWorkspace($workspaceId, [
            'password' => '<PASSWORD>',
        ]);

        self::assertInstanceOf(WorkspaceWithCredentialsInterface::class, $workspace);
        self::assertSame($workspaceId, $workspace->getWorkspaceId());
        self::assertSame($workspaceData['connection']['backend'], $workspace->getBackendType());
        self::assertSame($workspaceData['backendSize'], $workspace->getBackendSize());
        self::assertSame($workspaceData['connection']['loginType'], $workspace->getLoginType()->value);

        self::assertSame([
            'host' => $workspaceData['connection']['host'],
            'warehouse' => $workspaceData['connection']['warehouse'],
            'database' => $workspaceData['connection']['database'],
            'schema' => $workspaceData['connection']['schema'],
            'user' => $workspaceData['connection']['user'],
            'password' => '<PASSWORD>', // the password is whatever is provided to getExistingWorkspace method
            'privateKey' => null,
            'account' => 'keboola',
        ], $workspace->getCredentials());
    }

    public function testResetWorkspaceCredentialsWithKeyPair(): void
    {
        $keyPairGen = new SnowflakeKeypairGenerator(new PemKeyCertificateGenerator());
        $originalKeyPair = $keyPairGen->generateKeyPair();

        $workspaceData = $this->createWorkspace([
            'backend' => 'snowflake',
            'networkPolicy' => 'system',
            'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
            'publicKey' => $originalKeyPair->publicKey,
        ]);
        $workspaceId = (string) $workspaceData['id'];

        // fetch the workspace through the workspace provider, as any client would do
        $workspace = $this->workspaceProvider->getExistingWorkspace($workspaceId, null);
        $newCredentials = $this->workspaceProvider->resetWorkspaceCredentials($workspace);

        self::assertArrayHasKey('privateKey', $newCredentials);
        self::assertStringStartsWith('-----BEGIN PRIVATE KEY-----', $newCredentials['privateKey']);
    }

    public function testResetWorkspaceCredentialsWithPassword(): void
    {
        $workspaceData = $this->createWorkspace([
            'backend' => 'snowflake',
            'networkPolicy' => 'system',
        ]);
        $workspaceId = (string) $workspaceData['id'];
        $originalPassword = $workspaceData['connection']['password'];

        // fetch the workspace through the workspace provider, as any client would do
        $workspace = $this->workspaceProvider->getExistingWorkspace($workspaceId, null);
        $newCredentials = $this->workspaceProvider->resetWorkspaceCredentials($workspace);

        self::assertArrayHasKey('password', $newCredentials);
        self::assertNotSame($originalPassword, $newCredentials['password']);
    }

    public function testCleanupExistingWorkspace(): void
    {
        $workspaceData = $this->createWorkspace([
            'backend' => 'snowflake',
            'networkPolicy' => 'system',
        ]);
        $workspaceId = (string) $workspaceData['id'];

        $this->workspaceProvider->cleanupWorkspace($workspaceId);

        try {
            $this->workspacesApiClient->getWorkspace((int) $workspaceId);
            self::fail('Workspace should not exist after cleanup');
        } catch (Throwable $e) {
            self::assertInstanceOf(ClientException::class, $e);
            self::assertSame(404, $e->getCode());
        }
    }

    /** @doesNotPerformAssertions  */
    public function testCleanupNonExistingWorkspace(): void
    {
        $this->workspaceProvider->cleanupWorkspace('non-existing-workspace-id');
    }
}
