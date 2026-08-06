plugins {
    java
    `java-library`
    `maven-publish`
    signing
    checkstyle
    id("org.springframework.boot") version "3.5.4" apply false
    id("io.spring.dependency-management") version "1.1.7"
    id("com.diffplug.spotless") version "7.0.4"
}

group = "io.github.erick9125"

// Overridden at release time with -PoutboxVersion=0.1.0. Assigning a literal here would win over a
// plain -Pversion=, so the property is explicit about what it overrides. Maven Central rejects
// SNAPSHOT versions, so a release must set this.
version = providers.gradleProperty("outboxVersion").getOrElse("0.1.0-SNAPSHOT")
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
    repositories {
        // A local directory in Maven Repository Layout, which is exactly what the Central Portal
        // wants zipped up. Publishing here also writes the .md5 and .sha1 files it expects.
        maven {
            name = "stagingDeploy"
            url =
                layout.buildDirectory
                    .dir("staging-deploy")
                    .get()
                    .asFile
                    .toURI()
        }
    }

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

// Signing is required by Maven Central and configured only when the key is present, so a normal
// build — and `check` — works without any secrets. The key is the ASCII-armoured private key, passed
// in through the environment rather than read from a keyring, which is what CI can do.
signing {
    // Environment first for CI, Gradle property as a fallback for running a release by hand. Both
    // are read rather than a keyring, so nothing depends on the machine's GPG setup.
    val signingKey =
        providers
            .environmentVariable("SIGNING_KEY")
            .orElse(providers.gradleProperty("signingKey"))
            .orNull
    val signingPassword =
        providers
            .environmentVariable("SIGNING_PASSWORD")
            .orElse(providers.gradleProperty("signingPassword"))
            .orNull

    if (!signingKey.isNullOrBlank()) {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications["mavenJava"])
    }
}

// Central rejects unsigned releases, and an unsigned bundle is only discovered after the upload.
// Failing here keeps a release from getting that far, while leaving SNAPSHOT builds unsigned so
// `check` and local work need no key at all.
val requireSignedRelease by tasks.registering {
    description = "Fails a release bundle that carries no signatures"
    group = "publishing"

    val isSnapshot = version.toString().endsWith("SNAPSHOT")
    val hasKey =
        !providers
            .environmentVariable("SIGNING_KEY")
            .orElse(providers.gradleProperty("signingKey"))
            .orNull
            .isNullOrBlank()

    doLast {
        if (!isSnapshot && !hasKey) {
            throw GradleException(
                "Refusing to build a release bundle without a signing key: Maven Central rejects " +
                    "unsigned artifacts. Set SIGNING_KEY and SIGNING_PASSWORD, or build a SNAPSHOT.",
            )
        }
    }
}

// The Central Portal takes a zip of the Maven layout: POST it to
// https://central.sonatype.com/api/v1/publisher/upload with the file in a `bundle` form field.
// maven-metadata files are excluded because the bundle describes one version, not a repository.
// The staging directory is a plain Maven repository, so it accumulates: without wiping it first, a
// release bundle carries whatever earlier builds left behind — unsigned SNAPSHOT artifacts included.
val cleanStagingDeploy by tasks.registering(Delete::class) {
    description = "Empties the staging repository so a bundle contains only this build"
    group = "publishing"
    delete(layout.buildDirectory.dir("staging-deploy"))
}

tasks.named("publishMavenJavaPublicationToStagingDeployRepository") {
    dependsOn(cleanStagingDeploy)
}

val centralBundle by tasks.registering(Zip::class) {
    description = "Builds the deployment bundle the Central Portal expects"
    group = "publishing"
    dependsOn(requireSignedRelease)
    dependsOn(tasks.named("publishMavenJavaPublicationToStagingDeployRepository"))

    from(layout.buildDirectory.dir("staging-deploy"))
    exclude("**/maven-metadata*")
    archiveFileName.set("central-bundle-$version.zip")
    destinationDirectory.set(layout.buildDirectory.dir("central"))

    doLast {
        logger.lifecycle("Central bundle: ${archiveFile.get().asFile}")
    }
}
