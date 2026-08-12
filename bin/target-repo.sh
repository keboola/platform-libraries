#!/usr/bin/env bash
set -e

if [[ -z ${1+x} ]]; then
  echo "Usage: target-repo.sh <library>"
  echo ""
  echo " <library> Library directory name under libs/ (e.g. key-generator)"
  echo ""
  echo "Prints the name of the standalone GitHub repository the library is published to."
  exit 1
fi

# Most libraries publish to a standalone repo of the same name; these are the exceptions where the
# target repo name differs from the library directory. This is the only place the mapping lives —
# both the split-library and create-release actions call this script.
case "${1}" in
  git-service-api-client)       echo "git-service-php-api-client" ;;
  key-generator)                echo "php-key-generator" ;;
  query-service-api-client)     echo "query-service-api-php-client" ;;
  sandboxes-service-api-client) echo "sandboxes-service-api-php-client" ;;
  vault-api-client)             echo "vault-api-php-client" ;;
  *)                            echo "${1}" ;;
esac
