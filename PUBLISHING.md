## Publishing artifacts to Maven Central

Dgraph owns the `io.dgraph` namespace on Maven Central. See [JIRA ticket][jira] for details.
This document contains instructions to publish dgraph4j build artefacts to Maven central

[jira]: https://issues.sonatype.org/browse/OSSRH-35895

### Before Deploying

* Get access to credentials for `dgraph` JIRA account on Maven Central

* Generate GPG credentials. Make sure you set a passphrase.
  - Note down the short version of the Key ID: `gpg --list-keys --keyid-format short`
  - Generate a secret key ring file if not present: `gpg --export-secret-keys -o /path/to/.gnupg/secring.gpg`
  - Publish the keys to the MIT server: `gpg --send-keys <key-id>` (Maven Central will check for keys here)

* Create `~/.gradle/gradle.properties` and populate it with all the credentials:
```
signing.keyId=<…keyId…>
signing.password=<…password…>
signing.secretKeyRingFile=</path/to/.gnupg/secring.gpg>

ossrhUsername=dgraph
ossrhPassword=<…password…>
```

### Deploying
* Build and test the code that needs to be published
* Bump version by modifying the `version` variable in `build.gradle` file.
* Run `./gradlew uploadArchives`
* Release the deployment by following the steps on the page _Releasing the Deployment_ (link in references below)

### References
* [Deploying to OSSRH with Gradle - Introduction](http://central.sonatype.org/pages/gradle.html)
* [StackOverflow thread on issues during signing artifacts](https://stackoverflow.com/questions/27936119/gradle-uploadarchives-task-unable-to-read-secret-key)
* [Releasing the Deployment](http://central.sonatype.org/pages/releasing-the-deployment.html)
