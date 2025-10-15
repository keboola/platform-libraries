<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Fixtures\Dynamic;

use Keboola\PhpTestUtils\Fixtures\FixtureTraits\StorageApiAwareTrait;
use Keboola\StorageApi\DevBranches;

class BranchFixture implements FixtureInterface
{
    use StorageApiAwareTrait;

    private string $branchId;

    public function initialize(): void
    {
        $branchesApi = new DevBranches($this->getStorageClientWrapper()->getBasicClient());
        $branch = $branchesApi->createBranch(uniqid('BranchFixture'));
        assert(is_scalar($branch['id']));
        $this->branchId = (string) $branch['id'];
    }

    public function cleanUp(): void
    {
        $branchesApi = new DevBranches($this->getStorageClientWrapper()->getBasicClient());
        $branchesApi->deleteBranch((int) $this->branchId);
    }

    public function getBranchId(): string
    {
        return $this->branchId;
    }
}
