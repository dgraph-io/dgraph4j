## Publishing Artifacts to Maven Central

Dgraph owns the `io.dgraph` namespace on Maven Central. Releases are published automatically via the
`cd-dgraph4j` GitHub Actions workflow, which uploads to Maven Central through the
[Sonatype Central Publisher Portal](https://central.sonatype.com).

### Prerequisites

The following GitHub Actions secrets must be configured in the repository:

| Secret                 | Description                                                                 |
| ---------------------- | --------------------------------------------------------------------------- |
| `OSSRH_USERNAME`       | Sonatype Central Portal user token username                                 |
| `OSSRH_PASSWORD`       | Sonatype Central Portal user token password                                 |
| `GPG_SIGNING_KEY`      | ASCII-armored GPG private key (`gpg --armor --export-secret-keys <key-id>`) |
| `GPG_SIGNING_PASSWORD` | Passphrase for the GPG signing key                                          |

Portal user tokens can be generated at https://central.sonatype.com/usertoken.

The GPG public key must be published to a well-known keyserver (e.g. `keys.openpgp.org`,
`keyserver.ubuntu.com`, or `pgp.mit.edu`) so that Maven Central can verify artifact signatures.

### Release Process

1. Create a `prepare-for-release-vXX.X.X` branch from `main`.
2. Bump the `version` in `build.gradle`.
3. Update the download version in README for both Maven and Gradle.
4. Update the `Supported Versions` table in README.
5. Update the `dgraph4j version` in the `grpc-netty` table in README.
6. Update `CHANGELOG.md`.
7. Open a PR, get it reviewed, and merge to `main`.
8. Create a release tag on GitHub (e.g. `v25.0.0`) from the merged `main`.
9. Go to **Actions** → **cd-dgraph4j** → **Run workflow** and enter the release tag.
10. The workflow will build, test, sign, and publish the artifacts to Maven Central.
11. Verify the release at https://central.sonatype.com/namespace/io.dgraph.

### Local Publishing (for testing)

Developers can still publish from a local machine by setting credentials in
`~/.gradle/gradle.properties`:

```properties
ossrhUsername=<portal-token-username>
ossrhPassword=<portal-token-password>

signing.keyId=<gpg-key-id>
signing.password=<gpg-passphrase>
signing.secretKeyRingFile=</path/to/.gnupg/secring.gpg>
```

Then run:

```sh
./gradlew publishToSonatype closeAndReleaseSonatypeStagingRepository
```

### References

- [Sonatype Central Publisher Portal](https://central.sonatype.com)
- [Generating a Portal Token](https://central.sonatype.org/publish/generate-portal-token/)
- [Publishing via the OSSRH Staging API](https://central.sonatype.org/publish/publish-portal-gradle/)
- [gradle-nexus/publish-plugin](https://github.com/gradle-nexus/publish-plugin)
