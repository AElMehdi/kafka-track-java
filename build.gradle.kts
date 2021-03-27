plugins {
    java
    application

    // Generate Java classes from Avro (avsc) files
    id("com.commercehub.gradle.plugin.avro") version "0.9.1"
    id("com.github.johnrengelman.shadow") version "5.2.0"
    id("com.google.cloud.tools.jib") version "1.1.1"

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

    implementation("org.slf4j:slf4j-simple:1.7.26")

    // Use JUnit Jupiter API for testing.
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.4.2")
    // Use JUnit Jupiter Engine for testing.
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.4.2")

    testImplementation("org.assertj:assertj-core:3.15.0")
    testCompile("org.apache.kafka:kafka-streams-test-utils:2.4.1")
}

application {
    mainClassName = "kafka.track.java.stream.TransformStream"
//        classpath = sourceSets.main.runtimeClasspath
//        args = ['configuration/dev.properties']
}

tasks {
    jar {
        manifest {
            attributes(mapOf(
//                "Class-P
//                ath" to configurations.compile.collect { it.getName() }.join(' '),
                    "Main-Class" to "kafka.track.java.stream.TransformStream"
            ))
        }
    }

    shadowJar {
        archiveBaseName.set("kstreams-transform-standalone-1.0")
//        archiveBaseName.set("kstreams-transform-standalone-${archiveVersion.get()}.${archiveExtension.get()}")
    }

    test {
        useJUnitPlatform()
        testLogging {
            outputs.upToDateWhen { false }
            showStandardStreams = true
            // There is an issue with it
//            exceptionFormat = "full"
        }
    }
}