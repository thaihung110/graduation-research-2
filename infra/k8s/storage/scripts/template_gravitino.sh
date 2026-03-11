#!/bin/bash

set -eu

APP_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "${APP_DIR}" || exit 1

helm template openhouse-gravitino helm/gravitino -f config/gravitino.yaml > test_template/gravitino_template.yaml
