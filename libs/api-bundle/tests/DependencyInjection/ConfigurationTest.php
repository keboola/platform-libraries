<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\DependencyInjection;

use Generator;
use Keboola\ApiBundle\DependencyInjection\Configuration;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

class ConfigurationTest extends TestCase
{
    /**
     * @param array<mixed> $input
     * @return array<mixed>
     */
    private function process(array $input): array
    {
        return (new Processor())->processConfiguration(new Configuration(), [$input]);
    }

    public function testStorageClientOptionsAbsentByDefault(): void
    {
        $config = $this->process([]);

        self::assertArrayNotHasKey('storage_client_options', $config);
    }

    public function testStorageClientOptionsStringNormalizesToService(): void
    {
        $config = $this->process([
            'storage_client_options' => 'app.storage_options',
        ]);

        self::assertSame(['service' => 'app.storage_options'], $config['storage_client_options']);
    }

    public function testStorageClientOptionsObjectFormParsesScalars(): void
    {
        $config = $this->process([
            'storage_client_options' => [
                'backoff_max_tries' => 10,
                'aws_retries' => 5,
                'aws_debug' => false,
                'retry_on_maintenance' => true,
                'use_branch_storage' => false,
                'user_agent' => 'custom-ua/1.0',
                'logger' => 'monolog.logger.storage_api',
            ],
        ]);

        self::assertSame(
            [
                'backoff_max_tries' => 10,
                'aws_retries' => 5,
                'aws_debug' => false,
                'retry_on_maintenance' => true,
                'use_branch_storage' => false,
                'user_agent' => 'custom-ua/1.0',
                'logger' => 'monolog.logger.storage_api',
            ],
            $config['storage_client_options'],
        );
    }

    public function testStorageClientOptionsServiceWithIndividualOptionIsRejected(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('use either a service reference');

        $this->process([
            'storage_client_options' => [
                'service' => 'app.storage_options',
                'backoff_max_tries' => 10,
            ],
        ]);
    }

    public function testStorageClientOptionsRejectsNonIntegerBackoff(): void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->process([
            'storage_client_options' => [
                'backoff_max_tries' => 'not-a-number',
            ],
        ]);
    }

    #[DataProvider('negativeIntegerOptionProvider')]
    public function testStorageClientOptionsRejectsNegativeIntegers(string $option): void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->process([
            'storage_client_options' => [
                $option => -1,
            ],
        ]);
    }

    public static function negativeIntegerOptionProvider(): Generator
    {
        yield 'backoff_max_tries' => ['backoff_max_tries'];
        yield 'aws_retries' => ['aws_retries'];
    }
}
