#!/bin/bash
set -e
set -x

source scripts/functions.sh

startZero
start

./gradlew build

quit 0
