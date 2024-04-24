<?php

declare(strict_types=1);

use Keboola\StorageApi\Client;
use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/../vendor/autoload.php';

(new Dotenv())->usePutenv()->bootEnv(dirname(__DIR__).'/.env', 'dev', []);

$requiredEnvs = ['STORAGE_API_TOKEN', 'STORAGE_API_TOKEN_MASTER', 'STORAGE_API_URL'];

foreach ($requiredEnvs as $env) {
    if (empty(getenv($env))) {
        throw new Exception(sprintf('The "%s" environment variable is empty.', $env));
    }
}

$tokeEnvs = ['STORAGE_API_TOKEN', 'STORAGE_API_TOKEN_MASTER'];
foreach ($tokeEnvs as $tokenEnv) {
    $client = new Client(
        [
            'token' => (string) getenv($tokenEnv),
            'url' => (string) getenv('STORAGE_API_URL'),
        ],
    );
    $tokenInfo = $client->verifyToken();

    print(sprintf(
        'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.' . "\n",
        $tokenInfo['description'],
        $tokenInfo['id'],
        $tokenInfo['owner']['name'],
        $tokenInfo['owner']['id'],
        $client->getApiUrl(),
    ));
}
