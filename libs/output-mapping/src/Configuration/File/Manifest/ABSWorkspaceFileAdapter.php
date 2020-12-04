<?php

namespace Keboola\OutputMapping\Configuration\File\Manifest;

use \Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\OutputMapping\Exception\OutputOperationException;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Symfony\Component\Yaml\Yaml;

class ABSWorkspaceFileAdapter extends \Keboola\OutputMapping\Configuration\Adapter
{
    /**
     * @var BlobRestProxy
     */
    private $blobClient;

    /**
     * @var string workspace container name
     */
    private $container;

    /**
     * @var WorkspaceProviderInterface
     */
    private $workspaceProvider;

    public function __construct($format = 'json', $workspaceProvider = null)
    {
        parent::__construct($format);
        $this->workspaceProvider = $workspaceProvider;
        $credentials = $this->workspaceProvider->getCredentials(WorkspaceProviderInterface::TYPE_ABS);
        $this->blobClient = BlobRestProxy::createBlobService($credentials['connectionString']);
        $this->container = $credentials['container'];
    }

    /**
     *
     * Read configuration from file
     *
     * @param $file
     * @return array
     * @throws OutputOperationException
     */
    public function readFromFile($file)
    {
        $serialized = $this->getContents($file);
        if ($this->getFormat() == 'yaml') {
            $data = Yaml::parse($serialized);
        } elseif ($this->getFormat() == 'json') {
            $encoder = new JsonEncoder();
            $data = $encoder->decode($serialized, $encoder::FORMAT);
        } else {
            throw new OutputOperationException("Invalid configuration format {$this->format}.");
        }
        $this->setConfig($data);
        return $this->getConfig();
    }

    /**
     *
     * Write configuration to file in given format
     *
     * @param $file
     */
    public function writeToFile($file)
    {
        if ($this->getFormat() == 'yaml') {
            $serialized = Yaml::dump($this->getConfig(), 10);
            if ($serialized == 'null') {
                $serialized = '{}';
            }
        } elseif ($this->getFormat() == 'json') {
            $encoder = new JsonEncoder();
            $serialized = $encoder->encode(
                $this->getConfig(),
                $encoder::FORMAT,
                ['json_encode_options' => JSON_PRETTY_PRINT]
            );
        } else {
            throw new OutputOperationException("Invalid configuration format {$this->format}.");
        }
        $this->blobClient->createBlockBlob($this->container, $file, $serialized);
    }

    /**
     * @param $file
     * @return mixed
     * @throws OutputOperationException
     */
    public function getContents($file)
    {
        $blobResult = $this->blobClient->getBlob($this->container, $file);
        if (!$blobResult) {
            throw new OutputOperationException("File '$file' not found.");
        }
        return stream_get_contents($blobResult->getContentStream());
    }
}
