<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent;

use InvalidArgumentException;
use Keboola\MessengerBundle\ConnectionEvent\EventInterface;

/**
 * Application event as defined at https://github.com/keboola/connection/blob/24517a7486e7a0990bad19d80246947d8dd438ef/legacy-app/application/modules/core/events/Application.php
 */
class ApplicationEvent implements EventInterface
{
    public function __construct(
        public readonly string $name,
        public readonly int $idEvent,
        public readonly string $type,

        // identification
        public readonly ?int $idAdmin = null,
        public readonly ?int $idProject = null,
        public readonly ?int $idAccessToken = null,
        public readonly ?string $accessTokenName = null,

        public readonly ?string $description = null,
        public readonly ?string $message = null,
        public readonly ?string $component = null,
        public readonly ?string $runId = null,

        // relations
        public readonly ?int $idBucket = null,
        public readonly ?string $tableName = null,
        public readonly ?int $idAccount = null,
        public readonly ?int $idExport = null,
        public readonly ?string $configurationId = null,
        public readonly ?int $idWorkspace = null,
        public readonly ?array $fileIds = null,

        // target object
        public readonly ?string $objectType = null, // account, bucket, table ...
        public readonly ?string $objectName = null, // name of object at log time
        public readonly ?string $objectId = null, // reference to object - object id or uri

        // context data
        public readonly ?array $context = null,
        public readonly ?array $params = null,
        public readonly ?array $results = null,
        public readonly ?array $performance = null,

        public readonly ?int $idBranch = null,
        public readonly ?bool $sendNotificationEmail = null,
    ) {
    }

    public static function fromArray(array $data): ApplicationEvent
    {
        if (empty($data['name'])) {
            throw new InvalidArgumentException('Event is missing property "name"');
        }

        if (empty($data['idEvent'])) {
            throw new InvalidArgumentException('Event is missing property "idEvent"');
        }

        if (empty($data['type'])) {
            throw new InvalidArgumentException('Event is missing property "type"');
        }

        return new self(
            $data['name'],
            $data['idEvent'],
            $data['type'],
            isset($data['idAdmin']) ? (int) $data['idAdmin'] : null,
            isset($data['idProject']) ? (int) $data['idProject'] : null,
            isset($data['idAccessToken']) ? (int) $data['idAccessToken'] : null,
            isset($data['accessTokenName']) ? (string) $data['accessTokenName'] : null,
            isset($data['description']) ? (string) $data['description'] : null,
            isset($data['message']) ? (string) $data['message'] : null,
            isset($data['component']) ? (string) $data['component'] : null,
            isset($data['runId']) ? (string) $data['runId'] : null,
            isset($data['idBucket']) ? (int) $data['idBucket'] : null,
            isset($data['tableName']) ? (string) $data['tableName'] : null,
            isset($data['idAccount']) ? (int) $data['idAccount'] : null,
            isset($data['idExport']) ? (int) $data['idExport'] : null,
            isset($data['configurationId']) ? (string) $data['configurationId'] : null,
            isset($data['idWorkspace']) ? (int) $data['idWorkspace'] : null,
            isset($data['fileIds']) ? (array) $data['fileIds'] : null,
            isset($data['objectType']) ? (string) $data['objectType'] : null,
            isset($data['objectName']) ? (string) $data['objectName'] : null,
            isset($data['objectId']) ? (string) $data['objectId'] : null,
            isset($data['context']) ? (array) $data['context'] : null,
            isset($data['params']) ? (array) $data['params'] : null,
            isset($data['results']) ? (array) $data['results'] : null,
            isset($data['performance']) ? (array) $data['performance'] : null,
            isset($data['idBranch']) ? (int) $data['idBranch'] : null,
            isset($data['sendNotificationEmail']) ? (bool) $data['sendNotificationEmail'] : null,
        );
    }

    public function toArray(): array
    {
        return [
            'name' => $this->name,
            'idEvent' => $this->idEvent,
            'type' => $this->type,

            'idAdmin' => $this->idAdmin,
            'idProject' => $this->idProject,
            'idAccessToken' => $this->idAccessToken,
            'accessTokenName' => $this->accessTokenName,

            'description' => $this->description,
            'message' => $this->message,
            'component' => $this->component,
            'runId' => $this->runId,

            'idBucket' => $this->idBucket,
            'tableName' => $this->tableName,
            'idAccount' => $this->idAccount,
            'idExport' => $this->idExport,
            'configurationId' => $this->configurationId,
            'idWorkspace' => $this->idWorkspace,
            'fileIds' => $this->fileIds,

            'objectType' => $this->objectType,
            'objectName' => $this->objectName,
            'objectId' => $this->objectId,

            'context' => $this->context,
            'params' => $this->params,
            'results' => $this->results,
            'performance' => $this->performance,

            'idBranch' => $this->idBranch,
            'sendNotificationEmail' => $this->sendNotificationEmail,
        ];
    }

    public function getId(): string
    {
        return (string) $this->idEvent;
    }

    public function getEventName(): string
    {
        return $this->name;
    }
}
