#!/usr/bin/env php
<?php

declare(strict_types=1);

use Keboola\PlatformLibrariesCi\AffectedLibrariesResolver;

require __DIR__ . '/vendor/autoload.php';

$repoRoot = dirname(__DIR__, 2);

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

function gitCommitExists(string $repoRoot, string $ref): bool
{
    $cmd = sprintf(
        'git -C %s cat-file -e %s 2>/dev/null',
        escapeshellarg($repoRoot),
        escapeshellarg($ref . '^{commit}'),
    );
    exec($cmd, $output, $exitCode);
    return $exitCode === 0;
}

function gitSingleLine(string $repoRoot, string $subcommand): ?string
{
    $cmd = sprintf('git -C %s %s 2>/dev/null', escapeshellarg($repoRoot), $subcommand);
    exec($cmd, $output, $exitCode);
    if ($exitCode !== 0 || $output === []) {
        return null;
    }
    $line = trim($output[0]);
    return $line !== '' ? $line : null;
}

/**
 * Decide which commit to diff HEAD against.
 *
 * Normally this is github.event.before (the ref tip before the push). After a
 * force-push/rebase — or on a branch's first push — that SHA is zero or points to
 * an orphaned commit absent from the checkout, so before..HEAD cannot be computed.
 * Fall back to the merge-base with main to capture the branch's real changes instead
 * of retesting the whole monorepo. Returns null when no usable base exists (e.g. on
 * main itself), letting the caller test everything.
 */
function resolveBase(string $repoRoot, string $before): ?string
{
    $isZeroOrEmpty = $before === '' || preg_match('/^0{40}$/', $before) === 1;
    if (!$isZeroOrEmpty && gitCommitExists($repoRoot, $before)) {
        return $before;
    }

    $mainRef = null;
    foreach (['origin/main', 'main'] as $ref) {
        if (gitCommitExists($repoRoot, $ref)) {
            $mainRef = $ref;
            break;
        }
    }
    if ($mainRef === null) {
        return null;
    }

    $mergeBase = gitSingleLine($repoRoot, sprintf('merge-base %s HEAD', escapeshellarg($mainRef)));
    $head = gitSingleLine($repoRoot, 'rev-parse HEAD');
    if ($mergeBase === null || $mergeBase === $head) {
        return null;
    }
    return $mergeBase;
}

/**
 * @return list<string> affected library dirs (sorted), or all dirs on fallback
 */
function resolveAffected(string $repoRoot, string $before): array
{
    $packages = loadPackages($repoRoot);
    $resolver = new AffectedLibrariesResolver($packages);

    try {
        $base = resolveBase($repoRoot, $before);
        if ($base === null) {
            // No usable base (push to main itself, no main ancestry, ...) -> test everything.
            return $resolver->allDirs();
        }

        $changed = gitChangedPaths($repoRoot, $base);

        if ($resolver->isFallbackToAll($changed)) {
            return $resolver->allDirs();
        }

        return $resolver->resolve($changed);
    } catch (Throwable $e) {
        fwrite(STDERR, 'affected-libraries failed, falling back to all: ' . $e->getMessage() . "\n");
        return $resolver->allDirs();
    }
}

// Run only when executed directly as a CLI script (not when included by tests).
if (isset($argv[0]) && realpath($argv[0]) === realpath(__FILE__)) {
    echo json_encode(resolveAffected($repoRoot, $argv[1] ?? '')), "\n";
    exit(0);
}
