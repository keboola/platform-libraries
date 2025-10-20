<?php

declare(strict_types=1);

use Symfony\Component\Dotenv\Dotenv;

require_once __DIR__ . '/../vendor/autoload.php';

$dotEnv = new Dotenv();
$dotEnv->usePutenv();
$dotEnv->bootEnv(dirname(__DIR__) . '/.env', 'dev', []);

$requiredVars = [
    'STORAGE_API_TOKEN',
    'QUERY_API_URL',
    'STORAGE_API_URL',
];

foreach ($requiredVars as $var) {
    if (empty($_ENV[$var])) {
        throw new RuntimeException(
            sprintf('Environment variable %s is required for functional tests', $var),
        );
    }
}
