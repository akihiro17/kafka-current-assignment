plugins {
    java
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "org.example"
version = "0.0.3-pre"

repositories {
    mavenCentral()
}

dependencies {
    // https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
    implementation("org.apache.kafka:kafka-clients:3.8.0")
    // https://mvnrepository.com/artifact/org.apache.kafka/kafka-server-common
    implementation("org.apache.kafka:kafka-server-common:3.8.0")
    // https://mvnrepository.com/artifact/com.google.code.gson/gson
    implementation("com.google.code.gson:gson:2.11.0")
    // https://mvnrepository.com/artifact/javax.annotation/javax.annotation-api
    implementation("javax.annotation:javax.annotation-api:1.3.2")
    // https://mvnrepository.com/artifact/net.sf.jopt-simple/jopt-simple
    implementation("net.sf.jopt-simple:jopt-simple:5.0.4")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.apache.kafka:kafka-clients:3.7.0:test")
    testImplementation("org.mockito:mockito-core:3.+")
}

application {
    mainClass = "kafkacurrentassignment.Command"
}

tasks.test {
    useJUnitPlatform()
}

tasks.shadowJar {
    archiveClassifier.set("")
    mergeServiceFiles()
}
