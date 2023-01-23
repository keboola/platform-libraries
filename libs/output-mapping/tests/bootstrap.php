<?php

declare(strict_types=1);

use Keboola\StorageApi\Exception;
use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/../vendor/autoload.php';

if (file_exists(dirname(__DIR__).'/.env.local')) {
    (new Dotenv())->usePutenv(true)->bootEnv(dirname(__DIR__).'/.env.local', 'dev', []);
}

$requiredEnvs = ['STORAGE_API_URL', 'STORAGE_API_TOKEN', 'STORAGE_API_TOKEN_MASTER'];
foreach ($requiredEnvs as $env) {
    if (empty(getenv($env))) {
        throw new Exception(sprintf('Environment variable "%s" is empty', $env));
    }
}

if (getenv('RUN_SYNAPSE_TESTS')) {
    if (empty(getenv('SYNAPSE_STORAGE_API_TOKEN'))) {
        throw new Exception('SYNAPSE_STORAGE_API_TOKEN must be set for synapse tests');
    }
    if (empty(getenv('SYNAPSE_STORAGE_API_URL'))) {
        throw new Exception('SYNAPSE_STORAGE_API_URL must be set for synapse tests');
    }
}
