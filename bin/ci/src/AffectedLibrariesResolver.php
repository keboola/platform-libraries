<?php

declare(strict_types=1);

namespace Keboola\PlatformLibrariesCi;

class AffectedLibrariesResolver
{
    private const INFRA_PREFIXES = ['Dockerfile', 'docker-compose', '.github/', 'bin/', '.dockerignore'];
    private const INFRA_EXACT = ['composer.json', 'composer.lock'];

    /** @var array<string, array{name: string, devDeps: list<string>}> */
    private array $packages;

    /** @var array<string, string> map composerName => dir */
    private array $nameToDir;

    /** @var array<string, list<string>> reverse graph: dir => dirs that depend on it */
    private array $dependents;

    /**
     * @param array<string, array{name: string, devDeps: list<string>}> $packages keyed by dir
     */
    public function __construct(array $packages)
    {
        $this->packages = $packages;

        $this->nameToDir = [];
        foreach ($packages as $dir => $meta) {
            $this->nameToDir[$meta['name']] = $dir;
        }

        $this->dependents = [];
        foreach ($packages as $dir => $meta) {
            foreach ($meta['devDeps'] as $depName) {
                if (!isset($this->nameToDir[$depName])) {
                    continue;
                }
                $depDir = $this->nameToDir[$depName];
                $this->dependents[$depDir][] = $dir;
            }
        }
    }

    /**
     * @param list<string> $changedPaths
     * @return list<string> sorted unique dirs
     */
    public function resolve(array $changedPaths): array
    {
        $changedDirs = [];
        foreach ($changedPaths as $path) {
            if (preg_match('#^libs/([^/]+)/#', $path, $m) === 1 && isset($this->packages[$m[1]])) {
                $changedDirs[$m[1]] = true;
            }
        }

        $affected = [];
        foreach (array_keys($changedDirs) as $dir) {
            $this->collectWithDependents((string) $dir, $affected);
        }

        $result = array_map('strval', array_keys($affected));
        sort($result);
        return $result;
    }

    /**
     * @param array<string, bool> $affected
     */
    private function collectWithDependents(string $dir, array &$affected): void
    {
        if (isset($affected[$dir])) {
            return;
        }
        $affected[$dir] = true;
        foreach ($this->dependents[$dir] ?? [] as $dependentDir) {
            $this->collectWithDependents($dependentDir, $affected);
        }
    }

    /**
     * @param list<string> $changedPaths
     */
    public function isFallbackToAll(array $changedPaths): bool
    {
        foreach ($changedPaths as $path) {
            if (str_starts_with($path, 'libs/')) {
                continue;
            }
            foreach (self::INFRA_PREFIXES as $prefix) {
                if (str_starts_with($path, $prefix)) {
                    return true;
                }
            }
            if (in_array($path, self::INFRA_EXACT, true)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return list<string> sorted canonical dir list
     */
    public function allDirs(): array
    {
        $dirs = array_map('strval', array_keys($this->packages));
        sort($dirs);
        return $dirs;
    }
}
