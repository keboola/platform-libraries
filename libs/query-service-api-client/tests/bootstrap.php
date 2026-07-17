<?php

declare(strict_types=1);

use Symfony\Component\Dotenv\Dotenv;

require_once __DIR__ . '/../vendor/autoload.php';

$envFile = dirname(__DIR__) . '/.env';
if (file_exists($envFile)) {
    (new Dotenv())->usePutenv()->bootEnv($envFile, 'dev', []);
}
