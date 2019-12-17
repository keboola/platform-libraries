FROM php:5.6
ENV DEBIAN_FRONTEND noninteractive
ENV COMPOSER_PROCESS_TIMEOUT 1200

RUN apt-get update -q \
  && apt-get install unzip git zlib1g-dev -y

RUN docker-php-ext-install zip

RUN cd \
  && curl -sS https://getcomposer.org/installer | php \
  && ln -s /root/composer.phar /usr/local/bin/composer

WORKDIR /code