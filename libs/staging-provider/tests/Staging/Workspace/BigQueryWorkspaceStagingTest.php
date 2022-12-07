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
                'schema' => 'schema',
                'warehouse' => '',
                'credentials' => '{"foo":"value"}}',
            ],
        ]);

        $credentials = $staging->getCredentials();
        self::assertSame(
            [
                'schema' => 'schema',
                'credentials' => '{"foo":"value"}}',
            ],
            $staging->getCredentials()
        );
    }
}
