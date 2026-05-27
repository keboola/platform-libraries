<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Auth;

interface AuthInterface
{
    /**
     * @return array<string, string>
     */
    public function getAuthenticationHeaders(): array;
}
