<?php

declare(strict_types=1);

namespace Keboola\ServiceClient;

use RuntimeException;

enum Service
{
    case AI;
    case BILLING;
    case BUFFER;
    case CONNECTION;
    case ENCRYPTION;
    case IMPORT;
    case NOTIFICATION;
    case OAUTH;
    case QUEUE;
    case QUEUE_INTERNAL_API;
    case SANDBOXES_API;
    case SANDBOXES_SERVICE;
    case SCHEDULER;
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
            self::ENCRYPTION => 'encryption',
            self::IMPORT => 'import',
            self::NOTIFICATION => 'notification',
            self::OAUTH => 'oauth',
            self::QUEUE => 'queue',
            self::QUEUE_INTERNAL_API => throw new RuntimeException('Job queue internal API does not have public DNS'),
            self::SANDBOXES_API => 'sandboxes',
            self::SANDBOXES_SERVICE => 'data-science',
            self::SCHEDULER => 'scheduler',
            self::SYNC_ACTIONS => 'sync-actions',
            self::TEMPLATES => 'templates',
            self::VAULT => 'vault',
        };
    }

    public function getInternalServiceName(): string
    {
        return match ($this) {
            self::AI => 'ai-service-api.default',
            self::BILLING => 'billing-api.default',
            self::BUFFER => 'buffer-api.buffer',
            self::CONNECTION => 'connection-api.connection',
            self::ENCRYPTION => 'encryption-api.default',
            self::IMPORT => 'sapi-importer.default',
            self::NOTIFICATION => 'notification-api.default',
            self::OAUTH => 'oauth-api.default',
            self::QUEUE => 'job-queue-api.default',
            self::QUEUE_INTERNAL_API => 'job-queue-internal-api.default',
            self::SANDBOXES_API => 'sandboxes-api.sandboxes',
            self::SANDBOXES_SERVICE => 'sandboxes-service-api.default',
            self::SCHEDULER => 'scheduler-api.default',
            self::SYNC_ACTIONS => 'runner-sync-api.default',
            self::TEMPLATES => 'templates-api.templates-api',
            self::VAULT => 'vault-api.default',
        };
    }
}
