<?php

declare(strict_types=1);

use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/../vendor/autoload.php';

(new Dotenv())
    ->usePutenv()
    ->bootEnv(dirname(__DIR__).'/.env', 'dev', [])
;
