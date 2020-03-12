plugins {
    java
    application
}

repositories {
    jcenter()
}

dependencies {
    // This dependency is used by the application.
    implementation("com.google.guava:guava:28.0-jre")

    // Use JUnit Jupiter API for testing.
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.4.2")

    // Use JUnit Jupiter Engine for testing.
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.4.2")
}

application {
    mainClassName = "kafka.track.java.App"
}

val test by tasks.getting(Test::class) {
    // Use junit platform for unit tests
    useJUnitPlatform()
}
