# Output Mapping 
[![Build Status](https://travis-ci.org/keboola/output-mapping.svg?branch=master)](https://travis-ci.org/keboola/output-mapping) 
[![Code Climate](https://codeclimate.com/github/keboola/output-mapping/badges/gpa.svg)](https://codeclimate.com/github/keboola/output-mapping) 
[![Test Coverage](https://codeclimate.com/github/keboola/output-mapping/badges/coverage.svg)](https://codeclimate.com/github/keboola/output-mapping/coverage)

Output mapping library for Docker Runner. Library processes output mapping, imports data to Storage tables into CSV files and files to Storage Files. 
Files are imported from local directory.


## Development


```
git clone https://github.com/keboola/output-mapping
cd output-mapping
```

Copy `.env.template` to `.env` and fill parameters.


```
docker build . -t keboola/output-mapping
docker run --volume $(pwd):/code keboola/output-mapping composer install
docker run --volume $(pwd):/code --env-file .env keboola/output-mapping ./vendor/bin/phpunit 

```