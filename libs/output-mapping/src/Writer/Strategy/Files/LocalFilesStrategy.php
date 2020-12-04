<?php


namespace Keboola\OutputMapping\Writer\Strategy\Files;


use Keboola\OutputMapping\Configuration\File\Manifest\Adapter as FileAdapter;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;

class LocalFilesStrategy extends AbstractFilesStrategy implements FilesStrategyInterface
{
    public function getManifestFiles($dir)
    {
        $finder = new Finder();
        $manifests = $finder->files()->name('*.manifest')->in($dir)->depth(0);
        $manifestFileNames = [];
        /** @var SplFileInfo $manifest */
        foreach ($manifests as $manifest) {
            $manifestFileNames[] = $manifest->getPathname();
        }
        return $manifestFileNames;
    }

    public function getFiles($source)
    {
        $finder = new Finder();
        /** @var SplFileInfo[] $foundFiles */
        $foundFiles = $finder->files()->notName('*.manifest')->in($source)->depth(0);
        $files = [];
        foreach ($foundFiles as $file) {
            $files[] = (new File())->setFileName($file->getFilename())->setPath($file->getPath());
        }
        return $files;
    }

    /**
     * @param $source
     * @param array $storageConfig
     * @throws \Keboola\StorageApi\ClientException
     */
    public function uploadFile($source, array $storageConfig = [])
    {
        $options = new FileUploadOptions();
        $options
            ->setTags(array_unique($storageConfig['tags']))
            ->setIsPermanent($storageConfig['is_permanent'])
            ->setIsEncrypted($storageConfig['is_encrypted'])
            ->setIsPublic($storageConfig['is_public'])
            ->setNotify($storageConfig['notify']);
        $this->clientWrapper->getBasicClient()->uploadFile($source, $options);
    }

    public function readFileManifest($manifestFile)
    {
        $adapter = new FileAdapter($this->format);
        try {
            return $adapter->readFromFile($manifestFile);
        } catch (\Exception $e) {
            throw new InvalidOutputException(
                sprintf('Failed to parse manifest file "%s" as "%s": %s', $source, $this->format, $e->getMessage()),
                $e->getCode(),
                $e
            );
        }
    }
}
