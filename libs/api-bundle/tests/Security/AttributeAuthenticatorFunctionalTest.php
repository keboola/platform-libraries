<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security;

use Keboola\ApiBundle\Security\AttributeAuthenticator;
use PHPUnit\Framework\TestCase;

class AttributeAuthenticatorFunctionalTest extends TestCase
{
    public function testSupports(): void
    {
        $authenticator = new AttributeAuthenticator();
    }
}
