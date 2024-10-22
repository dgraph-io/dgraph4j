#!/usr/bin/env bash
version=$(mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)

mvn deploy && \
git tag "v${version}" master && \
git push origin --tags
