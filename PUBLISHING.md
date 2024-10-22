## Publishing artefacts to Maven Central

Dgraph owns the `io.dgraph` namespace on Maven Central. See [JIRA ticket][jira] for details.
This document contains instructions to publish dgraph4j build artefacts to Maven central.

[jira]: https://issues.sonatype.org/browse/OSSRH-35895

### Before Deploying

* Get access to credentials for `dgraph` JIRA account on Maven Central.
* Generate GPG credentials. Make sure you set a passphrase. You can use this
[guide](https://help.github.com/en/articles/generating-a-new-gpg-key).
* Note down the short version of the Key ID: `gpg --list-keys --keyid-format short`.
* Generate a secret key ring file if not present: `gpg --export-secret-keys -o /path/to/.gnupg/secring.gpg`.
* Publish the keys to the MIT server: `gpg --send-keys <key-id>` (Maven Central will check for keys here).
* Create `~/.m2/settings.xml` and populate it with all the credentials:
```
<settings>
  <servers>
    <server>
      <id>ossrh</id>
      <username>dgraph</username>
      <password>password</password>
    </server>
  </servers>
  <profiles>
    <profile>
      <id>ossrh</id>
      <properties>
        <gpg.executable>gpg</gpg.executable>
        <gpg.passphrase>password</gpg.passphrase>
      </properties>
    </profile>
  </profiles>
  <activeProfiles>
    <activeProfile>ossrh</activeProfile>
  </activeProfiles>
</settings>
```

### Deploying
* Build and test the code that needs to be published.
* Bump version by modifying the `version` variable in `pom.xml` file.
* Update download version in README for Maven.
* Update `Supported Versions` table in README.
* Update `dgraph4j version` in `grpc-netty` table in README.
* Update CHANGELOG.
* Raise a PR for the above changes. Put the changelog in PR description and merge it.
* Run `mvn deploy`.
* Release the deployment by following the steps on the page _Releasing the Deployment_ (link in references below).
* Also cut a release tag on GitHub with the new version.

### References
* [Publishing a project on Maven Central](https://medium.com/@nmauti/publishing-a-project-on-maven-central-8106393db2c3)
* [Deploying to OSSRH with Maven - Introduction](http://central.sonatype.org/pages/apache-maven.html)
* [StackOverflow thread on issues during signing artefacts](https://stackoverflow.com/questions/27936119/gradle-uploadarchives-task-unable-to-read-secret-key)
* [Releasing the Deployment](http://central.sonatype.org/pages/releasing-the-deployment.html)
