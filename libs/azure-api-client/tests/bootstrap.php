<?php

declare(strict_types=1);

use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/../vendor/autoload.php';

$envFile = dirname(__DIR__) . '/.env.local';
// Mutant runs exclude the e2e group, and under Infection's stream wrapper root in the container cannot load the
// host user's 0600 file, which would fail every mutant run.
if (is_file($envFile) && getenv('INFECTION') === false) {
    (new Dotenv())->usePutenv()->load($envFile);
}
