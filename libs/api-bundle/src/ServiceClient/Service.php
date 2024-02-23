<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\ServiceClient;

enum Service
{
    case AI;
    case BILLING;
    case BUFFER;
    case CONNECTION;
    case DATA_SCIENCE;
    case ENCRYPTION;
    case IMPORT;
    case MLFLOW;
    case NOTIFICATION;
    case OAUTH;
    case QUEUE;
    case SANDBOXES;
    case SCHEDULER;
    case SPARK;
    case SYNC_ACTIONS;
    case TEMPLATES;
    case VAULT;

    public function getPublicSubdomain(): string
    {
        return match ($this) {
            self::AI => 'ai',
            self::BILLING => 'billing',
            self::BUFFER => 'buffer',
            self::CONNECTION => 'connection',
            self::DATA_SCIENCE => 'data-science',
            self::ENCRYPTION => 'encryption',
            self::IMPORT => 'import',
            self::MLFLOW => 'mlflow', // ?
            self::NOTIFICATION => 'notification',
            self::OAUTH => 'oauth',
            self::QUEUE => 'queue',
            self::SANDBOXES => 'sandboxes',
            self::SCHEDULER => 'scheduler',
            self::SPARK => 'spark', // ?
            self::SYNC_ACTIONS => 'sync-actions',
            self::TEMPLATES => 'templates',
            self::VAULT => 'vault',
        };
    }

    public function getInternalServiceName(): string
    {
        return match ($this) {
            self::AI => 'ai-service-api.default',
            self::BILLING => 'billing-api.buffer',
            self::BUFFER => 'buffer-api.default',
            self::CONNECTION => 'connection-api.connection',
            self::DATA_SCIENCE => 'sandboxes-service-api.default',
            self::ENCRYPTION => 'encryption-api.default',
            self::IMPORT => 'sapi-importer.default',
            self::MLFLOW => 'mlflow.default', // ??
            self::NOTIFICATION => 'notification-api.default',
            self::OAUTH => 'oauth-api.default',
            self::QUEUE => 'job-queue-api.default',
            self::SANDBOXES => 'sandboxes-api.sandboxes',
            self::SCHEDULER => 'scheduler-api.default',
            self::SPARK => 'spark.default', // ??
            self::SYNC_ACTIONS => 'runner-sync-api.default',
            self::TEMPLATES => 'templates-api.templates-api',
            self::VAULT => 'vault-api.default',
        };
    }
}
