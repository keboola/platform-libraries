<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security;

use Keboola\ApiBundle\Attribute\StorageTokenAuth;

#[StorageTokenAuth(['foo-feature'])]
class AuthenticatedController
{
    public function __invoke(): void
    {

    }
}
