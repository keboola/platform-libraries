<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\StorageApiClient;

use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken;
use RuntimeException;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Controller\ValueResolverInterface;
use Symfony\Component\HttpKernel\ControllerMetadata\ArgumentMetadata;
use Symfony\Component\Security\Core\Authentication\Token\Storage\TokenStorageInterface;

class StorageClientApiFactoryResolver implements ValueResolverInterface
{
    public function __construct(
        private readonly StorageClientRequestFactory $storageClientRequestFactory,
        private readonly TokenStorageInterface $tokenStorage,
    ) {
    }

    public function resolve(Request $request, ArgumentMetadata $argument): iterable
    {
        if ($argument->getType() !== StorageClientApiFactory::class) {
            return [];
        }

        $user = $this->tokenStorage->getToken()?->getUser();
        if ($user instanceof StorageApiToken) {
            return [new StorageClientApiFactory($this->storageClientRequestFactory, $request, $user)];
        }

        if ($argument->isNullable()) {
            return [null];
        }

        throw new RuntimeException(sprintf(
            'Cannot resolve argument "$%s": no authenticated Storage API token. Guard the '
            . 'controller with #[StorageApiTokenAuth], or make the argument nullable to allow null.',
            $argument->getName(),
        ));
    }
}
