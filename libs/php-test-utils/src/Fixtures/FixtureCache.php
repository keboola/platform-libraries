<?php

// phpcs:ignoreFile -- Generic.Files.OneObjectStructurePerFile

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Fixtures;

use Keboola\PhpTestUtils\Fixtures\Dynamic\FixtureInterface;

class FixtureCache
{
    /**
     * @var FixtureInterface[][]
     */
    private static array $reusableFixtures = [];
    /**
     * @var FixtureInterface[][][][]
     */
    private static array $fixtures = [];

    public static function add(
        FixtureInterface $fixture,
        string $fixtureName,
        BackendType $backendType,
        bool $isReusable,
        string $methodName,
        string $dataName,
    ): void
    {
        if ($isReusable) {
            self::$reusableFixtures[$fixtureName][$backendType->value] = $fixture;
        } else {
            self::$fixtures[$fixtureName][$backendType->value][$methodName][$dataName] = $fixture;
        }
    }

    public static function getReusable(string $fixtureName, BackendType $backendType): ?FixtureInterface
    {
        return self::$reusableFixtures[$fixtureName][$backendType->value] ?? null;
    }

    public static function init(): void
    {
        // Register a shutdown function to act as a destructor
        register_shutdown_function([self::class, 'destruct']);
    }

    public static function destruct(): void
    {
        array_walk_recursive(self::$fixtures, function (FixtureInterface $fixture) {
            $fixture->cleanUp();
        });

        array_walk_recursive(self::$reusableFixtures, function (FixtureInterface $fixture) {
            $fixture->cleanUp();
        });
    }
}

FixtureCache::init();
