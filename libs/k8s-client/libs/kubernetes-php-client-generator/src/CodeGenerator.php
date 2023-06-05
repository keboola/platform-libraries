<?php


namespace CodeGenerator;

use CodeGenerator\Code\API;
use CodeGenerator\Code\KubernetesExtentions;
use CodeGenerator\Code\KubernetesOperations;
use CodeGenerator\Code\Model;
use CodeGenerator\Code\ResponseTypes;
use OpenAPI\Schema\V2 as Schema;
use Psr\Log\LoggerInterface;

class CodeGenerator
{
    /** @var Schema\Swagger */
    private $Swagger;

    /** @var LoggerInterface */
    private $logger;

    public function __construct(Schema\Swagger $Swagger, LoggerInterface $logger)
    {
        $this->Swagger = $Swagger;
        $this->logger = $logger;
    }

    /**
     * @param Schema\Schema[] $definitions
     *
     * @throws \Exception
     */
    public function generateDefinitions($definitions)
    {
        foreach ($definitions as $name => $SchemaObject) {
            $Model = new Model($name, $SchemaObject);
            $this->logger->debug($Model->getFilename());
            $Model->write();
        }
    }

    /**
     * @param Schema\PathItem[] $pathItems
     */
    public function generateApis($pathItems)
    {
        /** @var Api[] $apiClassGenerators */
        $apiClassGenerators = [];
        $ungroupedApis      = [];

        foreach ($pathItems as $path => $PathItemObject) {
            $pathParameters = $PathItemObject->parameters;

            foreach (KubernetesOperations::OPERATIONS as $operation) {
                $OperationObject = $PathItemObject->$operation;
                if ($OperationObject instanceof Schema\Operation) {
                    /** @var Schema\Operation $OperationObject */
                    if (key_exists(KubernetesExtentions::ACTION, $OperationObject->getPatternedFields())
                        && key_exists(KubernetesExtentions::GROUP_VERSION_KIND, $OperationObject->getPatternedFields())
                    ) {
                        $apiKind = $OperationObject->getPatternedFields()
                                   [KubernetesExtentions::GROUP_VERSION_KIND][KubernetesExtentions::KIND];

                        if (array_key_exists($apiKind, $apiClassGenerators)) {
                            $API = $apiClassGenerators[$apiKind];
                        } else {
                            $API                          = new API($apiKind);
                            $apiClassGenerators[$apiKind] = $API;
                        }

                        $API->parseMethod($OperationObject, $path, $operation, $pathParameters);

                    } else {
                        $ungroupedApis[$path][$operation] = $OperationObject;
                    }
                }
            }
        }

        foreach ($apiClassGenerators as $ApiClassGenerator) {
            $this->logger->debug($ApiClassGenerator->getFilename());
            $ApiClassGenerator->write();
        }
    }

    /**
     * @param Schema\PathItem[] $pathItems
     */
    public function generateResponseTypes($pathItems)
    {
        $ResponseTypes = new ResponseTypes();
        foreach ($pathItems as $path => $PathItemObject) {
            foreach (KubernetesOperations::OPERATIONS as $operation) {
                $OperationObject = $PathItemObject->$operation;
                if ($OperationObject instanceof Schema\Operation) {
                    $ResponseTypes->parseReseponseTypes($OperationObject);
                }
            }
        }

        $ResponseTypes->write();
    }
}
