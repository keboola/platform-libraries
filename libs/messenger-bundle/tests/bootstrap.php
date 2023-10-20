<?php

declare(strict_types=1);

use Keboola\MessengerBundle\Tests\KeboolaMessengerBundleTestingKernel;
use Symfony\Component\Dotenv\Dotenv;

require __DIR__.'/../vendor/autoload.php';

(new Dotenv())->usePutenv()->loadEnv(__DIR__.'/../.env', testEnvs: []);

// boot kernel to warmup var/cache contents
$kernel = new KeboolaMessengerBundleTestingKernel($_SERVER['APP_ENV'], (bool) $_SERVER['APP_DEBUG']);
$kernel->boot();
