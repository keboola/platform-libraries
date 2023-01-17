<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\StorageApi\DevBranches;
use Keboola\StorageApiBranch\ClientWrapper;

trait CreateBranchTrait
{
    public function createBranch(ClientWrapper $clientWrapper, string $branchName): string
    {
        $branches = new DevBranches($clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === $branchName) {
                $branches->deleteBranch($branch['id']);
            }
        }
        return (string) $branches->createBranch($branchName)['id'];
    }
}
