#!/bin/bash
set -e

source scripts/functions.sh

startZero
start

./gradlew build

quit 0
