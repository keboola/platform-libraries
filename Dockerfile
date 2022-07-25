# syntax=docker/dockerfile:1.3

ARG PHP_VERSION=7.4

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

RUN curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin/ --filename=composer

RUN pecl install xdebug \
    && docker-php-ext-enable xdebug

FROM base AS input-mapping

ENV LIB_NAME=input-mapping
ENV LIB_HOME=/code/libs/${LIB_NAME}

ARG COMPOSER_MIRROR_PATH_REPOS=1

WORKDIR ${LIB_HOME}

COPY libs/${LIB_NAME}/composer.json ./
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader
COPY libs/${LIB_NAME} ./
RUN composer install $COMPOSER_FLAGS

FROM base AS staging-provider

ENV LIB_NAME=staging-provider
ENV LIB_HOME=/code/libs/${LIB_NAME}

ARG COMPOSER_MIRROR_PATH_REPOS=1

WORKDIR ${LIB_HOME}

COPY libs/${LIB_NAME}/composer.json ./
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader
COPY libs/${LIB_NAME} ./
RUN composer install $COMPOSER_FLAGS

FROM base AS dev
WORKDIR /code
