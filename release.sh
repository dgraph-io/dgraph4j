#!/usr/bin/env bash
version=$(gradle -q version)

gradle publish && \
git tag "v${version}" master && \
git push origin --tags

