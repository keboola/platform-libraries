<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\ServiceClient;

enum Service: string
{
    case AI = 'ai';
    case BILLING = 'billing';
    case BUFFER = 'buffer';
    case CONNECTION = 'connection';
    case DATA_SCIENCE = 'data-science';
    case ENCRYPTION = 'encryption';
    case IMPORT = 'import';
    case OAUTH = 'oauth';
    case MLFLOW = 'mlflow';
    case NOTIFICATION = 'notification';
    case SANDBOXES = 'sandboxes';
    case SCHEDULER = 'scheduler';
    case SPARK = 'spark';
    case SYNC_ACTIONS = 'sync-actions';
    case QUEUE = 'queue';
    case TEMPLATES = 'templates';
    case VAULT = 'vault';

    public function getPublicSubdomain(): string
    {
        return $this->value;
    }

    public function getInternalServiceName(): string
    {
        return match ($this) {
            self::DATA_SCIENCE => 'sandboxes-service',
            default => $this->value,
        };
    }

    public function getInternalServiceNamespace(): string
    {
        return match ($this) {
            self::BUFFER => 'buffer',
            self::CONNECTION => 'connection',
            self::SANDBOXES => 'sandboxes',
            self::TEMPLATES => 'templates-api',
            default => 'default',
        };
    }
}
