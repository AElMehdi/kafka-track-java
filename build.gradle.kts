plugins {
    java
    application

    id("com.commercehub.gradle.plugin.avro") version "0.9.1"
}

repositories {
    jcenter()
    mavenCentral()

    // Needed for avro serde dependency
    maven(url = "http://packages.confluent.io/maven")
}

dependencies {
    implementation("org.apache.kafka:kafka-streams:2.2.0")

    compile("org.apache.avro:avro:1.8.2")
    implementation("io.confluent:kafka-streams-avro-serde:5.2.1")


    // Use JUnit Jupiter API for testing.
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.4.2")
    // Use JUnit Jupiter Engine for testing.
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.4.2")

    testImplementation("org.assertj:assertj-core:3.15.0")
    testCompile("org.apache.kafka:kafka-streams-test-utils:2.4.1")
}

application {
    mainClassName = "kafka.track.java.App"
}

val test by tasks.getting(Test::class) {
    // Use junit platform for unit tests
    useJUnitPlatform()
}
