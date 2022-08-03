<?php

declare(strict_types=1);

use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/../vendor/autoload.php';

if (file_exists(dirname(__DIR__).'/.env.local')) {
    (new Dotenv())->usePutenv(true)->bootEnv(dirname(__DIR__).'/.env.local', 'dev', []);
}

$requiredEnvs = ['STORAGE_API_TOKEN', 'STORAGE_API_TOKEN_MASTER', 'STORAGE_API_URL'];

foreach ($requiredEnvs as $env) {
    if (empty(getenv($env))) {
        throw new Exception(sprintf('The "%s" environment variable is empty.', $env));
    }
}
