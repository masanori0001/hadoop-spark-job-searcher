#!/bin/bash

DEPLOY_DIR=$(cd "$(dirname $0)"; pwd)/..

cd ${DEPLOY_DIR}

mvn package
if [ $? -ne 0 ]; then
  echo "compilation error"
  exit 1
fi

mkdir -p dist

cp ${DEPLOY_DIR}/target/histlogmon-jar-with-dependencies.jar ${DEPLOY_DIR}/dist/
