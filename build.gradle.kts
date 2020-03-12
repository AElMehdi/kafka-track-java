plugins {
    java
    application
}

repositories {
    jcenter()
    mavenCentral()
}

dependencies {

    // Use JUnit Jupiter API for testing.
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.4.2")
    // Use JUnit Jupiter Engine for testing.
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.4.2")

    testRuntimeOnly("org.assertj:assertj-core:3.15.0")
}

application {
    mainClassName = "kafka.track.java.App"
}

val test by tasks.getting(Test::class) {
    // Use junit platform for unit tests
    useJUnitPlatform()
}
