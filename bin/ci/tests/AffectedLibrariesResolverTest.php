<?php

declare(strict_types=1);

namespace Keboola\PlatformLibrariesCi\Tests;

use Generator;
use Keboola\PlatformLibrariesCi\AffectedLibrariesResolver;
use PHPUnit\Framework\TestCase;

class AffectedLibrariesResolverTest extends TestCase
{
    /**
     * @param array<string, array{name: string, devDeps: list<string>}> $packages
     * @param list<string> $changedPaths
     * @param list<string> $expected
     * @dataProvider resolveProvider
     */
    public function testResolve(array $packages, array $changedPaths, array $expected): void
    {
        $resolver = new AffectedLibrariesResolver($packages);
        self::assertSame($expected, $resolver->resolve($changedPaths));
    }

    public static function resolveProvider(): Generator
    {
        $packages = self::samplePackages();

        yield 'single leaf change' => [
            'packages' => $packages,
            'changedPaths' => ['libs/settle/src/Foo.php'],
            'expected' => ['input-mapping', 'output-mapping', 'settle'],
        ];

        yield 'transitive through staging-provider' => [
            'packages' => $packages,
            'changedPaths' => ['libs/staging-provider/src/X.php'],
            'expected' => ['input-mapping', 'output-mapping', 'staging-provider'],
        ];

        yield 'key-generator fans out to three dependents' => [
            'packages' => $packages,
            'changedPaths' => ['libs/key-generator/composer.json'],
            'expected' => ['input-mapping', 'key-generator', 'output-mapping', 'staging-provider'],
        ];

        yield 'package-name differs from dir is resolved' => [
            'packages' => $packages,
            'changedPaths' => ['libs/query-service-api-client/src/Q.php'],
            'expected' => ['query-service-api-client'],
        ];

        yield 'two unrelated changes are merged' => [
            'packages' => $packages,
            'changedPaths' => ['libs/slicer/src/A.php', 'libs/service-client/src/B.php'],
            'expected' => ['configuration-variables-resolver', 'output-mapping', 'service-client', 'slicer'],
        ];

        yield 'no lib change yields empty set' => [
            'packages' => $packages,
            'changedPaths' => ['README.md'],
            'expected' => [],
        ];

        yield 'unknown lib dir is ignored' => [
            'packages' => $packages,
            'changedPaths' => ['libs/does-not-exist/src/A.php'],
            'expected' => [],
        ];

        yield 'duplicate paths collapse to single dir' => [
            'packages' => $packages,
            'changedPaths' => ['libs/settle/src/A.php', 'libs/settle/src/B.php'],
            'expected' => ['input-mapping', 'output-mapping', 'settle'],
        ];
    }

    public function testInfraChangeTriggersFallbackToAll(): void
    {
        $resolver = new AffectedLibrariesResolver(self::samplePackages());
        self::assertTrue($resolver->isFallbackToAll(['docker-compose.yml']));
        self::assertTrue($resolver->isFallbackToAll(['Dockerfile']));
        self::assertTrue($resolver->isFallbackToAll(['.github/workflows/ci.yml']));
        self::assertTrue($resolver->isFallbackToAll(['bin/ci/src/AffectedLibrariesResolver.php']));
        self::assertTrue($resolver->isFallbackToAll(['composer.json']));
        self::assertFalse($resolver->isFallbackToAll(['libs/settle/src/Foo.php']));
        self::assertFalse($resolver->isFallbackToAll(['README.md', 'LICENSE']));
    }

    public function testAllDirsReturnsCanonicalSortedList(): void
    {
        $resolver = new AffectedLibrariesResolver(self::samplePackages());
        self::assertSame(
            [
                'configuration-variables-resolver', 'input-mapping', 'key-generator',
                'output-mapping', 'query-service-api-client', 'service-client', 'settle',
                'slicer', 'staging-provider', 'vault-api-client',
            ],
            $resolver->allDirs(),
        );
    }

    /**
     * @return array<string, array{name: string, devDeps: list<string>}>
     */
    private static function samplePackages(): array
    {
        return [
            'key-generator' => ['name' => 'keboola/key-generator', 'devDeps' => []],
            'settle' => ['name' => 'keboola/settle', 'devDeps' => []],
            'slicer' => ['name' => 'keboola/slicer', 'devDeps' => []],
            'service-client' => ['name' => 'keboola/service-client', 'devDeps' => []],
            'vault-api-client' => ['name' => 'keboola/vault-api-client', 'devDeps' => []],
            'query-service-api-client' => ['name' => 'keboola/query-api-php-client', 'devDeps' => []],
            'staging-provider' => ['name' => 'keboola/staging-provider', 'devDeps' => ['keboola/key-generator']],
            'input-mapping' => ['name' => 'keboola/input-mapping', 'devDeps' => [
                'keboola/key-generator', 'keboola/staging-provider', 'keboola/settle',
            ]],
            'output-mapping' => ['name' => 'keboola/output-mapping', 'devDeps' => [
                'keboola/input-mapping', 'keboola/key-generator', 'keboola/slicer', 'keboola/staging-provider',
            ]],
            'configuration-variables-resolver' => ['name' => 'keboola/configuration-variables-resolver', 'devDeps' => [
                'keboola/service-client', 'keboola/vault-api-client',
            ]],
        ];
    }
}
