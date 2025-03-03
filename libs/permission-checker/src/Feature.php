<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker;

enum Feature: string
{
    case QUEUE_V2 = 'queuev2';
    case PROTECTED_DEFAULT_BRANCH = 'protected-default-branch';
    case AI_AUTOMATIONS = 'ai-automations';
}
