plugins {
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
    implementation("org.apache.kafka:kafka-clients:3.7.0")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.apache.kafka:kafka-clients:3.7.0:test") {
        version {
            branch = "trunck"
        }
    }
    testImplementation("org.mockito:mockito-core:3.+")
}

application {
    mainClass = "kafkacurrentassignment.Command"
}

tasks.test {
    useJUnitPlatform()
}