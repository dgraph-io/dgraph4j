### Creating a Java project

```
mkdir DgraphJavaSample
cd DgraphJavaSample
gradle init --type java-application
```

Modify the `build.gradle` file to change the `repositories` and `dependencies`:

```groovy
// Apply the java plugin to add support for Java
apply plugin: 'java'

// Apply the maven plugin to add support for Maven
apply plugin: 'maven'

// Apply the application plugin to add support for building an application
apply plugin: 'application'

// Use maven to pull down dependencies
repositories {
  mavenCentral()
}

dependencies {
  // Use Dgraph Java client
  compile 'io.dgraph:dgraph4j:0.9.1'

  // Use JUnit test framework
  testCompile 'junit:junit:4.12'
}

// Define the main class for the application
mainClassName = 'App'
```
