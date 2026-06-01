#!/usr/bin/env php
<?php

declare(strict_types=1);

use Keboola\PlatformLibrariesCi\AffectedLibrariesResolver;

require __DIR__ . '/vendor/autoload.php';

$repoRoot = dirname(__DIR__, 2);
$base = $argv[1] ?? '';

/**
 * @return array<string, array{name: string, devDeps: list<string>}>
 */
function loadPackages(string $repoRoot): array
{
    $packages = [];
    foreach (glob($repoRoot . '/libs/*/composer.json') ?: [] as $composerPath) {
        $dir = basename(dirname($composerPath));
        $json = json_decode((string) file_get_contents($composerPath), true);
        if (!is_array($json) || !isset($json['name'])) {
            continue;
        }
        $devDeps = [];
        foreach (['require', 'require-dev'] as $section) {
            $constraints = $json[$section] ?? [];
            if (!is_array($constraints)) {
                continue;
            }
            foreach ($constraints as $pkg => $constraint) {
                if ($constraint === '*@dev' && str_starts_with((string) $pkg, 'keboola/')) {
                    $devDeps[] = (string) $pkg;
                }
            }
        }
        $packages[$dir] = ['name' => (string) $json['name'], 'devDeps' => array_values(array_unique($devDeps))];
    }
    ksort($packages);
    return $packages;
}

/**
 * @return list<string>
 */
function gitChangedPaths(string $repoRoot, string $base): array
{
    $cmd = sprintf('git -C %s diff --name-only %s', escapeshellarg($repoRoot), escapeshellarg($base));
    exec($cmd, $output, $exitCode);
    if ($exitCode !== 0) {
        throw new RuntimeException(sprintf('git diff failed (exit %d) for base "%s"', $exitCode, $base));
    }
    return array_values(array_filter(array_map('trim', $output), static fn (string $l): bool => $l !== ''));
}

try {
    $packages = loadPackages($repoRoot);
    $resolver = new AffectedLibrariesResolver($packages);

    // Zero SHA / empty base / first commit -> test everything.
    if ($base === '' || preg_match('/^0{40}$/', $base) === 1) {
        echo json_encode($resolver->allDirs()), "\n";
        exit(0);
    }

    $changed = gitChangedPaths($repoRoot, $base);

    if ($resolver->isFallbackToAll($changed)) {
        echo json_encode($resolver->allDirs()), "\n";
        exit(0);
    }

    echo json_encode($resolver->resolve($changed)), "\n";
    exit(0);
} catch (Throwable $e) {
    fwrite(STDERR, 'affected-libraries failed, falling back to all: ' . $e->getMessage() . "\n");
    $packages = loadPackages($repoRoot);
    echo json_encode((new AffectedLibrariesResolver($packages))->allDirs()), "\n";
    exit(0);
}
