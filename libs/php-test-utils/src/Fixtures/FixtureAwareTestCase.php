<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Fixtures;

use InvalidArgumentException;
use Keboola\PhpTestUtils\Fixtures\Dynamic\FixtureInterface;
use Keboola\PhpTestUtils\Fixtures\Dynamic\ReusableFixtures;
use Keboola\PhpTestUtils\Fixtures\FixtureTraits\EntityManagerTrait;
use Keboola\PhpTestUtils\Fixtures\FixtureTraits\KernelBrowserTrait;
use Keboola\PhpTestUtils\Fixtures\FixtureTraits\StorageApiAwareTrait;
use Keboola\PhpTestUtils\Fixtures\FixtureTraits\StorageTokenTrait;
use Keboola\PhpTestUtils\TestEnvVarsTrait;
use Monolog\Logger;
use PHPUnit\Framework\Attributes\Group;
use ReflectionClass;
use ReflectionObject;
use Symfony\Bundle\FrameworkBundle\KernelBrowser;
use Symfony\Bundle\FrameworkBundle\Test\BrowserKitAssertionsTrait;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;

#[Group('slow-tests')]
abstract class FixtureAwareTestCase extends WebTestCase
{
    use TestEnvVarsTrait;
    use BrowserKitAssertionsTrait;

    private static KernelBrowser $client;

    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();
        self::$client = self::createClient();
        self::bootKernel();
    }

    protected function setUp(): void
    {
        parent::setUp();

        /** @var Logger $logger */
        $logger = self::getContainer()->get('monolog.logger');
        $logger->info(
            sprintf(
                '%s %s::%s %s',
                str_repeat('-', 15),
                static::class,
                $this->name(),
                str_repeat('-', 15),
            ),
        );
    }

    protected static function getBrowser(): KernelBrowser
    {
        return self::$client;
    }

    private function isReusableTestMethod(): bool
    {
        $reflection = new ReflectionObject($this);
        return count(
            $reflection->getMethod($this->name())->getAttributes(ReusableFixtures::class),
        ) > 0;
    }

    private function getBackendType(): BackendType
    {
        $reflection = new ReflectionObject($this);
        $attributes = $reflection->getMethod($this->name())->getAttributes(FixtureBackend::class);
        if (count($attributes) === 0) {
            return BackendType::SNOWFLAKE;
        } else {
            /** @var FixtureBackend $attribute */
            $attribute = $attributes[0]->newInstance();
            return $attribute->backend;
        }
    }

    /**
     * @param ReflectionClass<object> $trait
     */
    protected function initializeTrait(
        FixtureInterface $fixture,
        ReflectionClass $trait,
        BackendType $backendType,
    ): void {
        if ($trait->getName() === StorageApiAwareTrait::class) {
            // @phpstan-ignore-next-line
            $fixture->createStorageClientWrapper(
                self::getRequiredEnv('HOSTNAME_SUFFIX'),
                $backendType === BackendType::SNOWFLAKE ?
                    self::getRequiredEnv('TEST_STORAGE_API_TOKEN_SNOWFLAKE') :
                    self::getRequiredEnv('TEST_STORAGE_API_TOKEN_BIGQUERY'),
            );
        }

        if ($trait->getName() === EntityManagerTrait::class) {
            $container = self::getContainer();
            // @phpstan-ignore-next-line
            $fixture->setEntityManager($container->get('doctrine')->getManager());
        }

        if ($trait->getName() === KernelBrowserTrait::class) {
            // @phpstan-ignore-next-line
            $fixture->setBrowser(self::$client);
        }

        if ($trait->getName() === StorageTokenTrait::class) {
            // @phpstan-ignore-next-line
            $fixture->setStorageToken(
                $backendType === BackendType::SNOWFLAKE ?
                    self::getRequiredEnv('TEST_STORAGE_API_TOKEN_SNOWFLAKE') :
                    self::getRequiredEnv('TEST_STORAGE_API_TOKEN_BIGQUERY'),
            );
        }
    }

    /**
     * @param class-string $fixtureName
     */
    private function createNewFixture(string $fixtureName, BackendType $backendType): FixtureInterface
    {
        $fixture = new $fixtureName();
        if (!$fixture instanceof FixtureInterface) {
            throw new InvalidArgumentException(sprintf(
                'Fixture "%s" must implement %s.',
                $fixtureName,
                FixtureInterface::class,
            ));
        }
        $class = new ReflectionClass($fixtureName);
        $traits = $class->getTraits();

        foreach ($traits as $trait) {
            $this->initializeTrait($fixture, $trait, $backendType);
        }
        $fixture->initialize();
        return $fixture;
    }

    /**
     * @template T of FixtureInterface
     * @param class-string<T> $fixtureName
     * @return T
     */
    protected function getFixture(string $fixtureName): FixtureInterface
    {
        $isReusable = $this->isReusableTestMethod();
        $backend = $this->getBackendType();

        if ($isReusable) {
            $fixture = FixtureCache::getReusable($fixtureName, $backend);
            if ($fixture !== null) {
                // @phpstan-ignore-next-line
                return $fixture;
            }
        }

        $fixture = $this->createNewFixture($fixtureName, $backend);
        FixtureCache::add(
            $fixture,
            $fixtureName,
            $backend,
            $isReusable,
            $this->name(),
            (string) $this->dataName(),
        );
        // @phpstan-ignore-next-line
        return $fixture;
    }
}
