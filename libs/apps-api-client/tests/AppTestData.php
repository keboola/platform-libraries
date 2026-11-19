<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient\Tests;

final class AppTestData
{
    public const DEPLOYMENT_ID = '019a8f2e-7c1d-7a3b-9e4f-5d6c7b8a9f01';

    /**
     * @param array<string, mixed> $overrides
     * @return array<string, mixed>
     */
    public static function app(array $overrides = []): array
    {
        return array_merge([
            'id' => '123',
            'projectId' => '456',
            'componentId' => 'keboola.data-apps',
            'branchId' => '789',
            'configId' => '1011',
            'type' => 'streamlit',
            'url' => 'https://my-app-123.hub.keboola.com',
            'hasManagedGitRepo' => false,
            'production' => [
                'desiredDeploymentId' => self::DEPLOYMENT_ID,
                'actualDeploymentId' => self::DEPLOYMENT_ID,
                'desiredConfigVersion' => '7',
                'actualConfigVersion' => '7',
                'desiredCreatedAt' => '2026-11-19T08:00:00+00:00',
            ],
            'state' => 'running',
        ], $overrides);
    }

    /**
     * @param array<string, mixed> $overrides
     * @return array<string, mixed>
     */
    public static function deployment(array $overrides = []): array
    {
        return array_merge([
            'id' => self::DEPLOYMENT_ID,
            'configVersion' => '7',
            'gitCommitSha' => null,
            'runtimeSize' => 'small',
            'buildState' => 'ready',
            'runState' => 'neverRun',
            'archived' => false,
            'archivedAt' => null,
            'createdAt' => '2026-11-19T08:00:00+00:00',
        ], $overrides);
    }
}
