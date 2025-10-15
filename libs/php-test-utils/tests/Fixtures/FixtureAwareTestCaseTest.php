<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Tests\Fixtures;

use Keboola\PhpTestUtils\Fixtures\Dynamic\FixtureInterface;
use Keboola\PhpTestUtils\Fixtures\Dynamic\ReusableFixtures;
use Keboola\PhpTestUtils\Fixtures\FixtureAwareTestCase;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Depends;

class FixtureAwareTestCaseTest extends FixtureAwareTestCase
{
    /**
     * Make TestKernel use our minimal kernel for tests.
     */
    protected static function getKernelClass(): string
    {
        return TestKernel::class;
    }

    public static function setUpBeforeClass(): void
    {
        // Do NOT call parent::setUpBeforeClass(), it would require BrowserKit (createClient()).
        static::bootKernel();
    }

    #[ReusableFixtures]
    public function testReusableSameInstanceWithinMethod(): FixtureInterface
    {
        $fixture1 = $this->getFixture(DummyFixture::class);
        $fixture2 = $this->getFixture(DummyFixture::class);

        self::assertSame($fixture1, $fixture2, 'Expected the same fixture instance within a reusable test method.');
        self::assertGreaterThanOrEqual(1, DummyFixture::$initializeCalls);

        return $fixture1;
    }

    #[ReusableFixtures]
    #[Depends('testReusableSameInstanceWithinMethod')]
    public function testReusableSameInstanceAcrossMethods(FixtureInterface $prev): void
    {
        $fixture = $this->getFixture(DummyFixture::class);
        self::assertSame(
            $prev,
            $fixture,
            'Expected the same reusable fixture instance across test methods.',
        );
    }

    /**
     * @return array<string, array<string>>
     */
    public static function nonReusableProvider(): array
    {
        return [
            'row A' => ['A'],
            'row B' => ['B'],
        ];
    }

    #[DataProvider('nonReusableProvider')]
    public function testNonReusableDifferentPerDataSet(string $row): void
    {
        /** @var array<int> $seen */
        static $seen = [];

        $fixture = $this->getFixture(DummyFixture::class);
        $id = spl_object_id($fixture);

        self::assertNotContains(
            $id,
            $seen,
            'Non-reusable fixture must be different for each data set.',
        );
        $seen[] = $id;
    }
}
