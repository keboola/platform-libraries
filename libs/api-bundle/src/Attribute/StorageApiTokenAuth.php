<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Attribute;

use Attribute;
use InvalidArgumentException;

#[Attribute(Attribute::TARGET_CLASS | Attribute::TARGET_METHOD | Attribute::IS_REPEATABLE)]
class StorageApiTokenAuth implements AuthAttributeInterface
{
    /**
     * @param array $features Require project to have all these features
     * @param null|bool $isAdmin If set to true, token must be admin, if set to false, token must not be admin
     * @param null|int-mask-of<StorageApiTokenRole::ROLE_*> $role Require token to have role matching the mash
     *     * StorageApiTokenRole::ROLE_ADMIN - token must have "admin" role
     *     * StorageApiTokenRole::ROLE_ADMIN | StorageApiTokenRole::ROLE_GUEST - token must have "admin" OR "guest" role
     *     * StorageApiTokenRole::ROLE_ANY & ~StorageApiTokenRole::ROLE_READ_ONLY - token must not have "readonly" role
     */
    public function __construct(
        public readonly array $features = [],
        public readonly ?bool $isAdmin = null,
        public readonly ?int $role = null,
    ) {
        if ($this->role !== null && $this->isAdmin === false) {
            throw new InvalidArgumentException(
                'Invalid combination of role AND isAdmin=false. Only admin tokens has roles',
            );
        }
    }
}
