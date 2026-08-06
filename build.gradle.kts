plugins {
    java
    `java-library`
    `maven-publish`
    checkstyle
    id("org.springframework.boot") version "3.5.4" apply false
    id("io.spring.dependency-management") version "1.1.7"
    id("com.diffplug.spotless") version "7.0.4"
}

group = "io.github.erick9125"
version = "0.1.0-SNAPSHOT"
description = "A reliable transactional outbox relay for Spring Boot"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
    withJavadocJar()
    withSourcesJar()
}

repositories {
    mavenCentral()
}

dependencyManagement {
    imports {
        mavenBom("org.springframework.boot:spring-boot-dependencies:3.5.4")
    }
    // The BOM is for resolving our own dependencies, not something to hand to consumers. Left on,
    // the plugin copies it into the published POM's dependencyManagement, which nudges a consuming
    // build's Spring Boot versions towards ours. The publication writes resolved versions on each
    // dependency instead, so nothing is lost by leaving it out.
    generatedPomCustomization {
        enabled(false)
    }
}

dependencies {
    api("org.springframework.boot:spring-boot-starter-jdbc")
    api("org.springframework.boot:spring-boot-autoconfigure")
    api("com.fasterxml.jackson.core:jackson-databind")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    // Exposed through the public constructors of OutboxMetrics, DefaultOutboxRelay and
    // DefaultOutboxPublisher, so consumers building those beans by hand need them to compile.
    api("io.micrometer:micrometer-core")
    api("io.micrometer:micrometer-observation")

    // Kafka is one broker adapter among others: consumers using a different broker must not be
    // forced to put spring-kafka on their classpath. KafkaOutboxPublisher and its auto-configured
    // bean are guarded by @ConditionalOnClass(KafkaTemplate.class).
    compileOnly("org.springframework.kafka:spring-kafka")

    implementation("org.slf4j:slf4j-api")

    compileOnly("org.springframework.boot:spring-boot-configuration-processor")
    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    // Flyway only applies the shipped schema in tests. Shipping it as a runtime dependency would
    // activate FlywayAutoConfiguration inside every consuming application.
    testImplementation("org.flywaydb:flyway-core")
    testImplementation("org.flywaydb:flyway-database-postgresql")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:kafka")
    testImplementation("org.assertj:assertj-core")
    testRuntimeOnly("org.postgresql:postgresql")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    systemProperty("spring.profiles.active", "test")
}

// `test` — and therefore `check` — runs everything, integration tests included. This is only a
// convenience for iterating without Docker; it is deliberately not wired into `check`, because a
// verification task that skips the integration tests would give a false sense of coverage.
tasks.register<Test>("unitTest") {
    description = "Runs the tests that do not need Docker"
    group = "verification"
    useJUnitPlatform()
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    filter {
        includeTestsMatching("*Test")
        excludeTestsMatching("*IntegrationTest")
    }
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
}

spotless {
    // The target must exclude build directories explicitly. A bare "examples/**" glob makes Spotless
    // read the example project's build output, which Gradle rejects as an undeclared task dependency
    // and fails `check` outright.
    java {
        target(
            fileTree(projectDir) {
                include("src/**/*.java", "examples/**/*.java")
                exclude("**/build/**", "**/.gradle/**")
            },
        )
        googleJavaFormat("1.27.0")
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
    }
    kotlinGradle {
        target(
            fileTree(projectDir) {
                include("*.gradle.kts", "examples/**/*.gradle.kts")
                exclude("**/build/**", "**/.gradle/**")
            },
        )
        ktlint()
    }
}

checkstyle {
    toolVersion = "10.21.4"
    isIgnoreFailures = false
    maxWarnings = 0
    configFile = file("config/checkstyle/checkstyle.xml")
}

// Publishing was broken for the whole life of the project and nothing noticed, because nothing ever
// tried to publish. `check` now generates the POM and asserts every dependency carries a version:
// without one the publication is rejected outright, and a forced publish hands consumers a POM they
// cannot resolve.
val verifyPublishedPom by tasks.registering {
    description = "Fails if the published POM would declare a dependency without a version"
    group = "verification"
    dependsOn(tasks.named("generatePomFileForMavenJavaPublication"))

    val pomFile = layout.buildDirectory.file("publications/mavenJava/pom-default.xml")
    inputs.file(pomFile)
    outputs.upToDateWhen { true }

    doLast {
        val pom = pomFile.get().asFile.readText()
        val unversioned =
            pom
                .split("<dependency>")
                .drop(1)
                .map { it.substringBefore("</dependency>") }
                .filterNot { it.contains("<version>") }
                .map { block ->
                    block.substringAfter("<artifactId>").substringBefore("</artifactId>")
                }

        if (unversioned.isNotEmpty()) {
            throw GradleException(
                "The published POM declares dependencies without a version: $unversioned. " +
                    "Consumers would not be able to resolve them.",
            )
        }
    }
}

tasks.named("check") {
    dependsOn("spotlessCheck", verifyPublishedPom)
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])

            // Dependencies are declared without versions because the Spring Boot BOM supplies them
            // through io.spring.dependency-management — but that is a resolution-time mechanism and
            // writes nothing into the published POM. Without this, publishing fails outright
            // ("Publication only contains dependencies without a version"), and a forced publish
            // would hand consumers a POM they cannot resolve.
            //
            // Resolved versions are written instead of exporting the BOM as a platform: making
            // consumers inherit all of spring-boot-dependencies would force their own Spring Boot
            // version to align with ours.
            versionMapping {
                usage("java-api") { fromResolutionResult() }
                usage("java-runtime") { fromResolutionResult() }
            }

            pom {
                name.set("Spring Outbox Relay")
                description.set(project.description)
                url.set("https://github.com/erick9125/spring-outbox-relay")
                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }
                developers {
                    developer {
                        id.set("erick9125")
                        name.set("Erick")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/erick9125/spring-outbox-relay.git")
                    developerConnection.set("scm:git:ssh://github.com:erick9125/spring-outbox-relay.git")
                    url.set("https://github.com/erick9125/spring-outbox-relay")
                }
            }
        }
    }
}
