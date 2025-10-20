<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Functional;

use Keboola\QueryApi\Client;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;
use Throwable;

abstract class BaseFunctionalTestCase extends TestCase
{

    protected Client $queryClient;
    protected ClientWrapper $clientWrapper;
    protected BranchAwareClient $branchAwareStorageClient;
    protected string $testBranchId;
    protected string $testWorkspaceId;

    protected function setUp(): void
    {
        parent::setUp();

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
                error_log('Failed to delete test workspace: ' . $e->getMessage());
            }
        }

        parent::tearDown();
    }

    private function initializeClients(): void
    {
        $storageApiToken = (string) getenv('STORAGE_API_TOKEN');
        $queryApiUrl = (string) getenv('QUERY_API_URL');
        $storageApiUrl = (string) getenv('STORAGE_API_URL');

        $this->queryClient = new Client([
            'url' => $queryApiUrl,
            'token' => $storageApiToken,
        ]);

        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                url: $storageApiUrl,
                token: $storageApiToken,
            ),
        );
    }

    private function findDefaultBranch(): void
    {
        $defaultBranch = $this->clientWrapper->getDefaultBranch();
        $this->testBranchId = (string) $defaultBranch->id;
        $this->branchAwareStorageClient = $this->clientWrapper->getBranchClient();
    }

    private function createTestWorkspace(): void
    {
        $workspaces = new Workspaces($this->branchAwareStorageClient);
        $workspaceData = $workspaces->createWorkspace([
            'name' => sprintf('query-test-workspace-%d', time()),
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
}
