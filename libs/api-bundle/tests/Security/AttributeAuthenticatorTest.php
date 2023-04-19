<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security;

use Keboola\ApiBundle\Security\AttributeAuthenticator;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;

class AttributeAuthenticatorTest extends TestCase
{
    public function testSupports(): void
    {
        $authenticator = new AttributeAuthenticator();
        $result = $authenticator->supports(new Request(attributes: [
            '_controller' => 'aa',
        ]));
    }
}
