#!/usr/bin/env bash
version=$(./gradlew -q version)

./gradlew publish && \
git tag "v${version}" master && \
git push origin --tags

