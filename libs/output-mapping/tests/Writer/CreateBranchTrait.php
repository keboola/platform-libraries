<?php

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\StorageApi\DevBranches;

trait CreateBranchTrait
{

    public function createBranch($clientWrapper, $branchName)
    {
        $branches = new DevBranches($clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === $branchName) {
                $branches->deleteBranch($branch['id']);
            }
        }
        return $branches->createBranch($branchName)['id'];
    }
}
