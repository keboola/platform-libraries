#!/usr/bin/env bash
set -Eeuo pipefail

# the env file and the Terraform outputs hold keys and the service principal secret
umask 077

ENV_FILE=".env.local"
INSERT_MODE=prepend
VERBOSE=false

help () {
  echo "Syntax: update-env.sh [-v] [-a] [-e ${ENV_FILE}]"
  echo "Options:"
  echo "  -a|--append         Append mode (used only when creating new env file, by default values are prepended to the env file)"
  echo "  -e|--env-file file  Env file to write (default: ${ENV_FILE})"
  echo "  -v|--verbose        Output extra information"
  echo ""
}

while [[ $# -gt 0 ]]; do
  case $1 in
    -a|--append)
      INSERT_MODE=append
      shift
      ;;
    -e|--env-file)
      ENV_FILE="$2"
      shift
      shift
      ;;
    -v|--verbose)
      VERBOSE=true
      shift
      ;;
    -h|--help)
      echo "Update env file with values from Terraform"
      echo ""
      help
      exit 0
      ;;
    *)
      echo "Unknown argument $1"
      echo ""
      help
      exit 1
      ;;
  esac
done

echo -e "Configuring \033[1;33m${ENV_FILE}\033[0m"

SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_PATH}/../.."
cd "${PROJECT_ROOT}"

DELIMITER_START="##>> BEGIN GENERATED CONTENT <<##"
DELIMITER_END="##>> END GENERATED CONTENT <<##"

if [ ! -f "${ENV_FILE}" ]; then
  echo "Creating missing env file"
  touch "${ENV_FILE}"
fi
chmod 600 "${ENV_FILE}"

if ! grep -q "${DELIMITER_START}" "${ENV_FILE}"; then
  if [[ $INSERT_MODE == "append" ]]; then
      echo "Appending new auto-generated section to env file"
      echo "" >> "${ENV_FILE}"
      echo "${DELIMITER_START}" >> "${ENV_FILE}"
      echo "${DELIMITER_END}" >> "${ENV_FILE}"
  else
      echo "Prepending new auto-generated section to env file"
      ENV=$(cat "${ENV_FILE}")
      echo "${DELIMITER_START}" > "${ENV_FILE}"
      echo "${DELIMITER_END}" >> "${ENV_FILE}"
      echo "" >> "${ENV_FILE}"
      echo "${ENV}" >> "${ENV_FILE}"
  fi
fi

if [ "${VERBOSE}" = true ]; then
  echo "Terraform outputs"
  terraform -chdir="${SCRIPT_PATH}" output
  echo ""
fi

echo "Building variables"
ENV_TF_FILE="${ENV_FILE}.tf"
TF_OUTPUTS_FILE="${SCRIPT_PATH}/tfoutput.json"
trap "rm ${TF_OUTPUTS_FILE} || true; rm ${ENV_TF_FILE} || true" EXIT
terraform -chdir="${SCRIPT_PATH}" output -json > "${TF_OUTPUTS_FILE}"

"${SCRIPT_PATH}/env-scripts/extract-variables.sh" > "${ENV_TF_FILE}"

echo "Writing variables"
if [[ "$OSTYPE" == "darwin"* ]];
then
  sed -i '' -e "/${DELIMITER_START}/,/${DELIMITER_END}/{ /${DELIMITER_START}/{p; r ${ENV_TF_FILE}
        }; /${DELIMITER_END}/p; d; }" "${ENV_FILE}";
else
  sed -i -e "/${DELIMITER_START}/,/${DELIMITER_END}/{ /${DELIMITER_START}/{p; r ${ENV_TF_FILE}
        }; /${DELIMITER_END}/p; d; }" "${ENV_FILE}";
fi

echo "Done"
