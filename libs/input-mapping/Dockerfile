FROM php:7.3-cli
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update -q \
  && apt-get install unzip git zlib1g-dev libzip-dev -y

RUN docker-php-ext-install zip

RUN cd \
  && curl -sS https://getcomposer.org/installer | php \
  && ln -s /root/composer.phar /usr/local/bin/composer

WORKDIR /code