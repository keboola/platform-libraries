<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient;

enum CredentialType: string
{
    case HttpToken = 'http_token';
    case SshKey = 'ssh_key';
}
