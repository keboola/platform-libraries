<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ClientFacadeFactory\Token;

use Keboola\K8sClient\ClientFacadeFactory\Token\InClusterToken;
use PHPUnit\Framework\TestCase;

class InClusterTokenTest extends TestCase
{
    public function testGetValueReadsFromFile(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'k8s-creds-test');
        file_put_contents($tmpFile, 'foo-token-1');

        $token = new InClusterToken($tmpFile);

        self::assertSame('foo-token-1', $token->getValue());
    }

    public function testGetValueTrimsFileContents(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'k8s-creds-test');
        file_put_contents($tmpFile, "\nfoo-token-1\n");

        $token = new InClusterToken($tmpFile);

        self::assertSame('foo-token-1', $token->getValue());
    }

    public function testGetValueReadsLazily(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'k8s-creds-test');
        file_put_contents($tmpFile, 'foo-token-1');

        $token = new InClusterToken($tmpFile);

        file_put_contents($tmpFile, 'foo-token-2');
        self::assertSame('foo-token-2', $token->getValue());
    }

    public function testGetValueCachesValue(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'k8s-creds-test');
        file_put_contents($tmpFile, 'foo-token-1');

        $token = new InClusterToken(
            $tmpFile,
            expirationTime: 2,
        );

        self::assertSame('foo-token-1', $token->getValue());

        file_put_contents($tmpFile, 'foo-token-2');
        self::assertSame('foo-token-1', $token->getValue());

        sleep(2);
        self::assertSame('foo-token-2', $token->getValue());
    }

    public function testGetValueThrowsExceptionOnReadError(): void
    {
        $token = new InClusterToken('non-existing-file');

        $this->expectExceptionMessage('Failed to read contents of in-cluster configuration file "non-existing-file"');
        $token->getValue();
    }
}
