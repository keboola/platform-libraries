<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient\Tests\Model;

use Generator;
use Keboola\AppsApiClient\Model\App;
use Keboola\AppsApiClient\Tests\AppTestData;
use PHPUnit\Framework\TestCase;
use TypeError;

class AppTest extends TestCase
{
    public function testFromResponseData(): void
    {
        $app = App::fromResponseData(AppTestData::app());

        self::assertSame('123', $app->id);
        self::assertSame('456', $app->projectId);
        self::assertSame('keboola.data-apps', $app->componentId);
        self::assertSame('789', $app->branchId);
        self::assertSame('1011', $app->configId);
        self::assertSame('streamlit', $app->type);
        self::assertSame('https://my-app-123.hub.keboola.com', $app->url);
        self::assertFalse($app->hasManagedGitRepo);
        self::assertSame(AppTestData::DEPLOYMENT_ID, $app->production->desiredDeploymentId);
        self::assertSame(AppTestData::DEPLOYMENT_ID, $app->production->actualDeploymentId);
        self::assertSame('7', $app->production->desiredConfigVersion);
        self::assertSame('7', $app->production->actualConfigVersion);
        self::assertSame('2026-11-19T08:00:00+00:00', $app->production->desiredCreatedAt);
        self::assertSame(App::STATE_RUNNING, $app->state);
    }

    public function testFromResponseDataOfNeverDeployedApp(): void
    {
        $app = App::fromResponseData(AppTestData::app([
            'branchId' => null,
            'type' => null,
            'url' => null,
            'production' => [
                'desiredDeploymentId' => null,
                'actualDeploymentId' => null,
                'desiredConfigVersion' => null,
                'actualConfigVersion' => null,
                'desiredCreatedAt' => null,
            ],
            'state' => 'notDeployed',
        ]));

        self::assertNull($app->branchId);
        self::assertNull($app->type);
        self::assertNull($app->url);
        self::assertNull($app->production->desiredDeploymentId);
        self::assertNull($app->production->actualDeploymentId);
        self::assertNull($app->production->desiredConfigVersion);
        self::assertNull($app->production->actualConfigVersion);
        self::assertNull($app->production->desiredCreatedAt);
        self::assertSame(App::STATE_NOT_DEPLOYED, $app->state);
    }

    /**
     * @dataProvider wrongTypeProvider
     */
    public function testFromResponseDataRejectsWrongType(array $data): void
    {
        $this->expectException(TypeError::class);

        App::fromResponseData($data);
    }

    public static function wrongTypeProvider(): Generator
    {
        yield 'id of wrong type' => [AppTestData::app(['id' => 123])];
        yield 'state of wrong type' => [AppTestData::app(['state' => null])];
        yield 'hasManagedGitRepo of wrong type' => [AppTestData::app(['hasManagedGitRepo' => 'false'])];
        yield 'production of wrong type' => [AppTestData::app(['production' => 'none'])];
        yield 'branchId of wrong type' => [AppTestData::app(['branchId' => 2])];
    }
}
