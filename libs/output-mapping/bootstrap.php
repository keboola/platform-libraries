<?php
// Define path to application directory
define('ROOT_PATH', __DIR__);
ini_set('display_errors', true);

date_default_timezone_set('Europe/Prague');

if (file_exists(__DIR__ . '/config.php')) {
    require_once __DIR__ . '/config.php';
}

require_once ROOT_PATH . '/vendor/autoload.php';

defined('STORAGE_API_TOKEN') || define('STORAGE_API_TOKEN', getenv('STORAGE_API_TOKEN') ?: 'your_token');
defined('STORAGE_API_URL') || define('STORAGE_API_URL', getenv('STORAGE_API_URL') ?: 'https://connection.keboola.com');
