FROM php:8.4

ENV COMPOSER_ALLOW_SUPERUSER 1
ARG COMPOSER_FLAGS="--prefer-dist --no-interaction --classmap-authoritative --no-scripts"

RUN apt-get update -q \
  && apt-get install git unzip \
  -y --no-install-recommends

RUN git config --global --add safe.directory /code
RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini
RUN curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin/ --filename=composer

WORKDIR /code/

COPY composer.json .
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader
COPY . .
RUN composer install $COMPOSER_FLAGS
