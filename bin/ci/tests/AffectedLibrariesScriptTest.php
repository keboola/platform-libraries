<?php

declare(strict_types=1);

namespace Keboola\PlatformLibrariesCi\Tests;

use PHPUnit\Framework\TestCase;
use function ob_get_clean;
use function ob_start;
use function resolveAffected;

// Load the script's functions without executing its CLI entrypoint (guarded by the
// realpath check at the bottom of the file). The shebang line is swallowed by ob_*.
ob_start();
require_once dirname(__DIR__) . '/affected-libraries.php';
ob_get_clean();

class AffectedLibrariesScriptTest extends TestCase
{
    /** @var list<string> */
    private array $repos = [];

    protected function tearDown(): void
    {
        foreach ($this->repos as $repo) {
            exec(sprintf('rm -rf %s', escapeshellarg($repo)));
        }
        $this->repos = [];
        parent::tearDown();
    }

    public function testValidBeforeDiffsOnlyThatPush(): void
    {
        $repo = $this->initRepo();
        $mainTip = $this->git($repo, 'rev-parse HEAD');
        $this->git($repo, 'checkout -q -b feature');
        $this->commitChange($repo, 'libs/lib-b/src/X.php', 'change-b', 'change b');

        self::assertSame(['lib-b'], resolveAffected($repo, $mainTip));
    }

    public function testOrphanBeforeFallsBackToMergeBase(): void
    {
        // Reproduces the force-push/rebase bug: `before` points to a commit that is not
        // present in the checkout, so before..HEAD is impossible -> diff against main.
        $repo = $this->initRepo();
        $this->git($repo, 'checkout -q -b feature');
        $this->commitChange($repo, 'libs/lib-b/src/X.php', 'change-b', 'change b');

        $orphan = '21b5f3b7950cc9fe16df59e3ab0398647e0a9554';
        self::assertSame(['lib-b'], resolveAffected($repo, $orphan));
    }

    public function testZeroBeforeFallsBackToMergeBase(): void
    {
        $repo = $this->initRepo();
        $this->git($repo, 'checkout -q -b feature');
        $this->commitChange($repo, 'libs/lib-a/src/X.php', 'change-a', 'change a');

        // lib-b depends on lib-a, so changing lib-a affects both.
        self::assertSame(['lib-a', 'lib-b'], resolveAffected($repo, str_repeat('0', 40)));
    }

    public function testOnMainTestsEverything(): void
    {
        // On main the merge-base equals HEAD -> no usable base -> test everything.
        $repo = $this->initRepo();
        self::assertSame(['lib-a', 'lib-b'], resolveAffected($repo, str_repeat('0', 40)));
    }

    public function testInfraChangeTestsEverything(): void
    {
        $repo = $this->initRepo();
        $mainTip = $this->git($repo, 'rev-parse HEAD');
        $this->git($repo, 'checkout -q -b feature');
        $this->commitChange($repo, 'composer.json', '{}', 'touch root composer.json');

        self::assertSame(['lib-a', 'lib-b'], resolveAffected($repo, $mainTip));
    }

    private function initRepo(): string
    {
        $repo = sys_get_temp_dir() . '/aff-' . bin2hex(random_bytes(6));
        mkdir($repo, 0777, true);
        $this->repos[] = $repo;

        $this->git($repo, 'init -q -b main');
        $this->git($repo, 'config user.email test@example.com');
        $this->git($repo, 'config user.name Test');
        $this->git($repo, 'config commit.gpgsign false');

        $this->writeComposer($repo, 'lib-a', ['name' => 'keboola/lib-a']);
        $this->writeComposer($repo, 'lib-b', ['name' => 'keboola/lib-b', 'require' => ['keboola/lib-a' => '*@dev']]);
        $this->git($repo, 'add -A');
        $this->git($repo, 'commit -q -m init');

        return $repo;
    }

    /**
     * @param array<string, mixed> $json
     */
    private function writeComposer(string $repo, string $lib, array $json): void
    {
        $dir = $repo . '/libs/' . $lib;
        mkdir($dir, 0777, true);
        file_put_contents($dir . '/composer.json', (string) json_encode($json));
    }

    private function commitChange(string $repo, string $path, string $content, string $message): void
    {
        $full = $repo . '/' . $path;
        $dir = dirname($full);
        if (!is_dir($dir)) {
            mkdir($dir, 0777, true);
        }
        file_put_contents($full, $content);
        $this->git($repo, 'add -A');
        $this->git($repo, 'commit -q -m ' . escapeshellarg($message));
    }

    private function git(string $repo, string $args): string
    {
        $command = sprintf('git -C %s %s 2>&1', escapeshellarg($repo), $args);
        exec($command, $output, $exitCode);
        self::assertSame(0, $exitCode, sprintf('git %s failed: %s', $args, implode("\n", $output)));
        return trim(implode("\n", $output));
    }
}
