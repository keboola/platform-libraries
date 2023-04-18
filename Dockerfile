# syntax=docker/dockerfile:1.3

ARG PHP_VERSION=8.1

FROM php:${PHP_VERSION}-cli AS base

ARG DEBIAN_FRONTEND=noninteractive
ENV COMPOSER_FLAGS="--prefer-dist --no-interaction"
ENV COMPOSER_ALLOW_SUPERUSER 1

COPY docker/php/php.ini /usr/local/etc/php/php.ini
COPY docker/php/xdebug.ini /usr/local/etc/php/conf.d/

RUN apt update -q \
 && apt install -y --no-install-recommends git zip unzip libzip4 libzip-dev zlib1g-dev \
 && docker-php-ext-install zip \
 && apt-get remove --autoremove -y libzip-dev zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

RUN curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin/ --filename=composer \
 && composer global config allow-plugins.symfony/flex true \
 && composer global require --no-progress --no-scripts --no-plugins symfony/flex

RUN pecl install xdebug \
 && docker-php-ext-enable xdebug


FROM base AS dev
WORKDIR /code


FROM base AS input-mapping
ARG COMPOSER_MIRROR_PATH_REPOS=1
ARG COMPOSER_HOME=/tmp/composer
ENV LIB_NAME=input-mapping
ENV LIB_HOME=/code/${LIB_NAME}
WORKDIR ${LIB_HOME}

COPY libs/${LIB_NAME}/composer.json ./
RUN --mount=type=bind,target=/libs,source=libs \
    --mount=type=cache,id=composer,target=${COMPOSER_HOME} \
    composer install $COMPOSER_FLAGS

COPY libs/${LIB_NAME} ./


FROM base AS staging-provider
ARG COMPOSER_MIRROR_PATH_REPOS=1
ARG COMPOSER_HOME=/tmp/composer
ENV LIB_NAME=staging-provider
ENV LIB_HOME=/code/${LIB_NAME}
WORKDIR ${LIB_HOME}

COPY libs/${LIB_NAME}/composer.json ./
RUN --mount=type=bind,target=/libs,source=libs \
    --mount=type=cache,id=composer,target=${COMPOSER_HOME} \
    composer install $COMPOSER_FLAGS

COPY libs/${LIB_NAME} ./


FROM base AS output-mapping
ARG COMPOSER_MIRROR_PATH_REPOS=1
ARG COMPOSER_HOME=/tmp/composer
ENV LIB_NAME=output-mapping
ENV LIB_HOME=/code/${LIB_NAME}
WORKDIR ${LIB_HOME}

COPY libs/${LIB_NAME}/composer.json ./
RUN --mount=type=bind,target=/libs,source=libs \
    --mount=type=cache,id=composer,target=${COMPOSER_HOME} \
    composer install $COMPOSER_FLAGS

COPY libs/${LIB_NAME} ./


FROM base AS configuration-variables-resolver
ARG COMPOSER_MIRROR_PATH_REPOS=1
ARG COMPOSER_HOME=/tmp/composer
ENV LIB_NAME=configuration-variables-resolver
ENV LIB_HOME=/code/${LIB_NAME}
WORKDIR ${LIB_HOME}

COPY libs/${LIB_NAME}/composer.json ./
RUN --mount=type=bind,target=/libs,source=libs \
    --mount=type=cache,id=composer,target=${COMPOSER_HOME} \
    composer install $COMPOSER_FLAGS

COPY libs/${LIB_NAME} ./


FROM base AS settle
ARG COMPOSER_MIRROR_PATH_REPOS=1
ARG COMPOSER_HOME=/tmp/composer
ENV LIB_NAME=settle
ENV LIB_HOME=/code/${LIB_NAME}
WORKDIR ${LIB_HOME}

COPY libs/${LIB_NAME}/composer.json ./
RUN --mount=type=bind,target=/libs,source=libs \
    --mount=type=cache,id=composer,target=${COMPOSER_HOME} \
    composer install $COMPOSER_FLAGS

COPY libs/${LIB_NAME} ./

FROM base AS logging-bundle
ARG SYMFONY_REQUIRE=6.*
ARG COMPOSER_MIRROR_PATH_REPOS=1
ARG COMPOSER_HOME=/tmp/composer
ENV LIB_NAME=logging-bundle
ENV LIB_HOME=/code/${LIB_NAME}
WORKDIR ${LIB_HOME}

COPY libs/${LIB_NAME}/composer.json ./
RUN --mount=type=bind,target=/libs,source=libs \
    --mount=type=cache,id=composer,target=${COMPOSER_HOME} \
    composer install $COMPOSER_FLAGS

COPY libs/${LIB_NAME} ./
