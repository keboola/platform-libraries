<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\ConnectionEvent\AuditLog\Admin;

use Keboola\MessengerBundle\ConnectionEvent\AuditLog\BaseAuditLogEvent;

class AdminDeletedEvent extends BaseAuditLogEvent
{
    public const NAME = 'auditLog.admin.deleted';

    private string $subjectAdminId;

    private string $subjectAdminName;

    private string $subjectAdminEmail;

    public static function fromArray(array $data): static
    {
        $event = parent::fromArray($data);

        $event->subjectAdminId = (string) $data['context']['admin']['id'];
        $event->subjectAdminName = $data['context']['admin']['name'];
        $event->subjectAdminEmail = $data['context']['admin']['email'];

        return $event;
    }

    public function toArray(): array
    {
        return parent::toArray() + [
            'context' => [
                'admin' => [
                    'id' => $this->subjectAdminId,
                    'name' => $this->subjectAdminName,
                    'email' => $this->subjectAdminEmail,
                ],
            ],
        ];
    }

    public function getSubjectAdminId(): string
    {
        return $this->subjectAdminId;
    }

    public function getSubjectAdminName(): string
    {
        return $this->subjectAdminName;
    }

    public function getSubjectAdminEmail(): string
    {
        return $this->subjectAdminEmail;
    }
}
