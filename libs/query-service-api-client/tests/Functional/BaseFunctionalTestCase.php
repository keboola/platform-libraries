<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Functional;

use Keboola\QueryApi\Client;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Workspaces;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;

abstract class BaseFunctionalTestCase extends TestCase
{
    protected Client $queryClient;
    protected StorageApiClient $storageApiClient;
    protected BranchAwareClient $branchAwareStorageClient;
    protected string $testBranchId;
    protected string $testWorkspaceId;

    protected function setUp(): void
    {
        parent::setUp();

        $this->validateEnvironmentVariables();
        $this->initializeClients();
        $this->findDefaultBranch();
        $this->createTestWorkspace();
    }

    protected function tearDown(): void
    {
        if (isset($this->testWorkspaceId)) {
            try {
                $workspaces = new Workspaces($this->branchAwareStorageClient);
                $workspaces->deleteWorkspace((int) $this->testWorkspaceId);
            } catch (Throwable $e) {
                // Log but don't fail the test if workspace cleanup fails
                error_log('Failed to delete test workspace: ' . $e->getMessage());
            }
        }

        parent::tearDown();
    }

    private function validateEnvironmentVariables(): void
    {
        $requiredVars = [
            'STORAGE_API_TOKEN',
            'QUERY_API_URL',
            'STORAGE_API_URL',
        ];

        foreach ($requiredVars as $var) {
            if (empty($_ENV[$var])) {
                throw new RuntimeException(
                    sprintf('Environment variable %s is required for functional tests', $var),
                );
            }
        }
    }

    private function initializeClients(): void
    {
        $storageApiToken = $_ENV['STORAGE_API_TOKEN'];
        $queryApiUrl = $_ENV['QUERY_API_URL'];
        $storageApiUrl = $_ENV['STORAGE_API_URL'];

        $this->queryClient = new Client([
            'url' => $queryApiUrl,
            'token' => $storageApiToken,
        ]);

        // Create Storage API client directly for tests
        $this->storageApiClient = new StorageApiClient([
            'url' => $storageApiUrl,
            'token' => $storageApiToken,
        ]);
    }

    private function findDefaultBranch(): void
    {
        $devBranches = new DevBranches($this->storageApiClient);

        // List all branches
        $branches = $devBranches->listBranches();

        // Find default branch
        $defaultBranch = null;
        foreach ($branches as $branch) {
            if (isset($branch['isDefault']) && $branch['isDefault'] === true) {
                $defaultBranch = $branch;
                break;
            }
        }

        if ($defaultBranch === null) {
            throw new RuntimeException('No default branch found');
        }

        $this->testBranchId = (string) $defaultBranch['id'];

        // Initialize branch-aware storage client
        $this->branchAwareStorageClient = $this->storageApiClient->getBranchAwareClient($this->testBranchId);
    }

    private function createTestWorkspace(): void
    {
        // Create a workspace for testing queries
        $workspaces = new Workspaces($this->branchAwareStorageClient);
        $workspaceData = $workspaces->createWorkspace([
            'name' => sprintf('query-test-workspace-%d', random_int(1000, 9999)),
            'backend' => 'snowflake',
        ], true);

        $this->testWorkspaceId = (string) $workspaceData['id'];
    }

    protected function getTestBranchId(): string
    {
        return $this->testBranchId;
    }

    protected function getTestWorkspaceId(): string
    {
        return $this->testWorkspaceId;
    }

    protected function createTestTable(?string $tableName = null): string
    {
        if ($tableName === null) {
            $tableName = 'test_table_' . random_int(1000, 9999);
        }

        // Create table and insert test data using Query Service
        $createTableSql = sprintf('
            CREATE OR REPLACE TABLE %s (
                id INTEGER PRIMARY KEY,
                name STRING,
                value INTEGER
            )', $tableName);

        $insertDataSql = sprintf('
            INSERT INTO %s (id, name, value) VALUES
            (1, \'test1\', 100),
            (2, \'test2\', 200),
            (3, \'test3\', 300)
        ', $tableName);

        // Execute table creation and data insertion
        $response = $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => [$createTableSql, $insertDataSql],
                'transactional' => true,
            ],
        );

        // Wait for completion
        assert(is_string($response['queryJobId']));
        $this->queryClient->waitForJobCompletion($response['queryJobId']);

        return $tableName;
    }
}
