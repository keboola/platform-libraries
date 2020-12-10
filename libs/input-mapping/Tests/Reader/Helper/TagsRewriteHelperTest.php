<?php

namespace Reader\Helper;

use Keboola\InputMapping\Reader\Helper\TagsRewriteHelper;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class TagsRewriteHelperTest extends TestCase
{
    /** @var ClientWrapper */
    private $clientWrapper;

    /** @var string */
    private $branchId;

    /** @var string */
    protected $tmpDir;

    public function setUp()
    {
        parent::setUp();

        // Create folders
        $temp = new Temp('docker');
        $temp->initRunFolder();
        $this->temp = $temp;
        $this->tmpDir = $temp->getTmpFolder();

        $this->clientWrapper = new ClientWrapper(
            new Client(['token' => STORAGE_API_TOKEN_MASTER, "url" => STORAGE_API_URL]),
            null,
            null
        );

        $branches = new DevBranches($this->clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === 'dev branch') {
                $branches->deleteBranch($branch['id']);
            }
        }
        $this->branchId = $branches->createBranch('dev branch')['id'];
    }

    public function testNoBranch()
    {
        $configuration = ['tags' => ['im-files-test']];

        $this->clientWrapper->setBranchId('');

        $testLogger = new TestLogger();
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags($configuration, $this->clientWrapper, $testLogger);

        self::assertSame($configuration, $expectedConfiguration);
    }

    public function testBranchRewriteFilesExists()
    {
        $root = $this->tmpDir;
        file_put_contents($root . '/upload', 'test');

        $branchTag = sprintf('%s-im-files-test', $this->branchId);

        $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag])
        );
        sleep(5);

        $this->clientWrapper->setBranchId($this->branchId);

        $configuration = ['tags' => ['im-files-test']];

        $testLogger = new TestLogger();
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags($configuration, $this->clientWrapper, $testLogger);

        self::assertTrue($testLogger->hasInfoThatContains(
            sprintf('Using dev tags "%s" instead of "im-files-test".', $branchTag)
        ));

        self::assertEquals([$branchTag], $expectedConfiguration['tags']);
    }

    public function testBranchRewriteSourceTagsFilesExists()
    {
        $root = $this->tmpDir;
        file_put_contents($root . '/upload', 'test');

        $branchTag = sprintf('%s-im-files-test', $this->branchId);

        $this->clientWrapper
            ->getBasicClient()
            ->uploadFile($root . '/upload', (new FileUploadOptions())->setTags([$branchTag]));
        sleep(5);

        $this->clientWrapper->setBranchId($this->branchId);

        $configuration = [
            'source' => [
                'tags' => [
                    [
                        'name' => 'im-files-test'
                    ]
                ],
            ],
        ];

        $testLogger = new TestLogger();
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags(
            $configuration,
            $this->clientWrapper,
            $testLogger
        );

        self::assertTrue(
            $testLogger->hasInfoThatContains(
                sprintf('Using dev source tags "%s" instead of "im-files-test".', $branchTag)
            )
        );

        self::assertEquals([['name' => $branchTag]], $expectedConfiguration['source']['tags']);
    }

    public function testBranchRewriteNoFiles()
    {
        $configuration = ['tags' => ['im-files-test']];

        $this->clientWrapper->setBranchId($this->branchId);

        $testLogger = new TestLogger();
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags($configuration, $this->clientWrapper, $testLogger);

        $branchTag = sprintf('%s-im-files-test', $this->branchId);

        self::assertFalse($testLogger->hasInfoThatContains(
            sprintf('Using dev tags "%s" instead of "im-files-test".', $branchTag)
        ));

        self::assertEquals($configuration, $expectedConfiguration);
    }

    public function testBranchRewriteSourceTagsNoFiles()
    {
        $configuration = [
            'source' => [
                'tags' => [
                    [
                        'name' => 'im-files-test'
                    ]
                ],
            ],
        ];

        $this->clientWrapper->setBranchId($this->branchId);

        $testLogger = new TestLogger();
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags(
            $configuration,
            $this->clientWrapper,
            $testLogger
        );

        $branchTag = sprintf('%s-im-files-test', $this->branchId);

        self::assertFalse(
            $testLogger->hasInfoThatContains(
                sprintf('Using dev source tags "%s" instead of "im-files-test".', $branchTag)
            )
        );

        self::assertEquals($configuration, $expectedConfiguration);
    }
}
