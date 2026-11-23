<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient\Tests;

final class AppTestData
{
    public const DEPLOYMENT_ID = '019a8f2e-7c1d-7a3b-9e4f-5d6c7b8a9f01';
    public const RUN_ID = '019a9a41-0b2c-7d3e-8f4a-1b2c3d4e5f60';

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
                'activeRunId' => self::RUN_ID,
            ],
            'state' => 'running',
        ], $overrides);
    }

    /**
     * @param array<string, mixed> $overrides
     * @return array<string, mixed>
     */
    public static function run(array $overrides = []): array
    {
        return array_merge([
            'id' => self::RUN_ID,
            'appId' => '123',
            'state' => 'running',
            'createdAt' => '2026-11-20T10:00:00+00:00',
            'startedAt' => '2026-11-20T10:00:30+00:00',
            'stoppedAt' => null,
            'startupLogs' => null,
            'runtimeSize' => 'small',
            'configVersion' => '7',
            'imageVersion' => '1.2.3',
            'failureReason' => null,
            'mode' => 'prod',
            'deploymentId' => self::DEPLOYMENT_ID,
            'subject' => ['type' => 'appDeployment', 'id' => self::DEPLOYMENT_ID],
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
            'activeRunId' => null,
            'archived' => false,
            'archivedAt' => null,
            'createdAt' => '2026-11-19T08:00:00+00:00',
        ], $overrides);
    }
}
