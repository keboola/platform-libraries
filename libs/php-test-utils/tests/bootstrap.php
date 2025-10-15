<?php

declare(strict_types=1);

use Keboola\ServiceClient\ServiceClient;
use Keboola\StorageApi\Client;
use Symfony\Component\Dotenv\Dotenv;

require dirname(__DIR__) . '/vendor/autoload.php';

$dotEnv = new Dotenv();
$dotEnv->usePutenv();
$dotEnv->bootEnv(dirname(__DIR__) . '/.env', 'dev', []);

$requiredEnvs = [
    'TEST_STORAGE_API_TOKEN_SNOWFLAKE',
    'HOSTNAME_SUFFIX',
];
foreach ($requiredEnvs as $env) {
    if (empty(getenv($env))) {
        throw new Exception(sprintf('The "%s" environment variable is empty.', $env));
    }
}

$tokenEnvVariables = [
    'TEST_STORAGE_API_TOKEN_SNOWFLAKE',
];

foreach ($tokenEnvVariables as $tokenEnvVariable) {
    $hostNameSuffix = (string) getenv('HOSTNAME_SUFFIX');
    assert($hostNameSuffix !== '');
    $client = new Client([
        'token' => getenv($tokenEnvVariable),
        'url' => new ServiceClient($hostNameSuffix)->getConnectionServiceUrl(),
    ]);
    $tokenInfo = $client->verifyToken();
    /** @var array{
     *      description: string,
     *      id: string,
     *      owner: array{
     *          name: string,
     *          id: string,
     *      }
     * } $tokenInfo */
    print(sprintf(
        'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.' . PHP_EOL,
        $tokenInfo['description'],
        $tokenInfo['id'],
        $tokenInfo['owner']['name'],
        $tokenInfo['owner']['id'],
        $client->getApiUrl(),
    ));
}
