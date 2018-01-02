#!/bin/bash
set -e
set -x

source scripts/functions.sh

startZero
start

./gradlew check jacocoTestReport coveralls

quit 0
