<?php

declare(strict_types=1);

use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/../vendor/autoload.php';

(new Dotenv())->usePutenv()->bootEnv(dirname(__DIR__).'/.env', 'dev', []);

$requiredEnvs = [
    'TEST_DATABASE_HOST',
    'TEST_DATABASE_PORT',
    'TEST_DATABASE_USER',
    'TEST_DATABASE_PASSWORD',
    'TEST_DATABASE_DB',
    'TEST_PROXY_HOST',
];
foreach ($requiredEnvs as $env) {
    if (empty(getenv($env))) {
        throw new Exception(sprintf('Environment variable "%s" is empty', $env));
    }
}
