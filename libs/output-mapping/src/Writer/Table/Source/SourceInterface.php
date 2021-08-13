<?php

namespace Keboola\OutputMapping\Writer\Table\Source;

use SplFileInfo;

interface SourceInterface
{
    /**
     * @return string
     */
    public function getSourceName();

    /**
     * @return string
     */
    public function getSourcePath();

    /**
     * @param SplFileInfo $file
     */
    public function setManifestFile(SplFileInfo $file = null);

    /**
     * @return null|SplFileInfo
     */
    public function getManifestFile();

    /**
     * @param null|array $mapping
     */
    public function setMapping(array $mapping = null);

    /**
     * @return null|array
     */
    public function getMapping();
}
