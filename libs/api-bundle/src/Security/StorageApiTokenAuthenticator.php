<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security;

use Keboola\ErrorControl\ErrorResponse;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\Factory\StorageClientRequestFactory;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\Exception\HttpException;
use Symfony\Component\Security\Core\Authentication\Token\TokenInterface;
use Symfony\Component\Security\Core\Exception\AuthenticationException;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;
use Symfony\Component\Security\Http\Authenticator\AbstractAuthenticator;
use Symfony\Component\Security\Http\Authenticator\Passport\Badge\UserBadge;
use Symfony\Component\Security\Http\Authenticator\Passport\Passport;
use Symfony\Component\Security\Http\Authenticator\Passport\SelfValidatingPassport;
use Symfony\Contracts\Service\ServiceSubscriberInterface;

class StorageApiTokenAuthenticator extends AbstractAuthenticator implements ServiceSubscriberInterface
{
    private StorageClientRequestFactory $clientRequestFactory;

    public function __construct(StorageClientRequestFactory $clientRequestFactory)
    {
        $this->clientRequestFactory = $clientRequestFactory;
    }

    public static function getSubscribedServices(): array
    {
        return [];
    }

    public function supports(Request $request): ?bool
    {
        return $request->headers->has(StorageClientRequestFactory::TOKEN_HEADER);
    }

    public function authenticate(Request $request): Passport
    {
        try {
            $wrapper = $this->clientRequestFactory->createClientWrapper($request);
            $storageApiClient = $wrapper->getBasicClient();
            $tokenInfo = $storageApiClient->verifyToken();
        } catch (ClientException $e) {
            throw new CustomUserMessageAuthenticationException($e->getMessage(), [], 0, $e);
        }

        $userBadge = new UserBadge(
            (string) $wrapper->getClientOptionsReadOnly()->getToken(),
            fn () =>  new StorageApiToken($tokenInfo, $storageApiClient->getTokenString())
        );

        return new SelfValidatingPassport($userBadge);
    }

    public function onAuthenticationSuccess(Request $request, TokenInterface $token, string $firewallName): ?Response
    {
        return null;
    }

    public function onAuthenticationFailure(Request $request, AuthenticationException $exception): ?Response
    {
        $errorMessage = $exception instanceof CustomUserMessageAuthenticationException ?
            $exception->getMessage() :
            'Authorization failed';

        return ErrorResponse::fromException(new HttpException(
            Response::HTTP_UNAUTHORIZED,
            $errorMessage,
            $exception
        ));
    }
}
