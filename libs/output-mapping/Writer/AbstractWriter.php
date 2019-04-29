<?php

namespace Keboola\OutputMapping\Writer;

use Keboola\StorageApi\Client;
use Psr\Log\LoggerInterface;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;

abstract class AbstractWriter
{
    /**
     * @var Client
     */
    protected $client;

    /**
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * @var string
     */
    protected $format = 'json';

    /**
     * AbstractWriter constructor.
     *
     * @param Client $client
     * @param LoggerInterface $logger
     */
    public function __construct(Client $client, LoggerInterface $logger)
    {
        $this->client = $client;
        $this->logger = $logger;
    }

    /**
     * @param string $format
     */
    public function setFormat($format)
    {
        $this->format = $format;
    }

    /**
     * @param string $dir
     * @return array
     */
    protected function getManifestFiles($dir)
    {
        $finder = new Finder();
        $manifests = $finder->files()->name('*.manifest')->in($dir)->depth(0);
        $manifestNames = [];
        /** @var SplFileInfo $manifest */
        foreach ($manifests as $manifest) {
            $manifestNames[] = $manifest->getPathname();
        }
        return $manifestNames;
    }
}
