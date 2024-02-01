<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Staging\Workspace;

use Keboola\StagingProvider\Staging\Workspace\BigQueryWorkspaceStaging;
use PHPUnit\Framework\TestCase;

class BigQueryWorkspaceStagingTest extends TestCase
{
    public function testGetCredentials(): void
    {
        $staging = new BigQueryWorkspaceStaging([
            'connection' => [
                'backend' => 'bigquery',
                'host' => '',
                'database' => '',
                'region' => 'US',
                'schema' => 'schema',
                'warehouse' => '',
                'credentials' => '{"foo":"value"}}',
            ],
        ]);

        self::assertSame(
            [
                'schema' => 'schema',
                'region' => 'US',
                'credentials' => '{"foo":"value"}}',
            ],
            $staging->getCredentials(),
        );
    }
}
