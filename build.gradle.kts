import com.gorylenko.GenerateGitPropertiesTask
import dev.vankka.dependencydownload.task.GenerateDependencyDownloadResourceTask
import org.gradle.kotlin.dsl.build
import org.gradle.kotlin.dsl.invoke
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    `maven-publish`
    `java-library`
    kotlin("jvm")
    alias(libs.plugins.shadow)
    alias(libs.plugins.gitProperties)

    id("provided-dependencies")

    id("dev.vankka.dependencydownload.plugin") version "2.0.0" apply false
}

allprojects {
    version = "0.0.1"
    group = ""

    repositories {
        mavenCentral()
        maven("https://repo.papermc.io/repository/maven-public/")
        maven("https://repo.mincats.eu/mirrors/")

        configureScalaRepository()
        configureScalaRepository(dev = true)
    }
}

subprojects {
    apply(plugin = "idea")
    apply(plugin = "java-library")
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "maven-publish")

    apply(plugin = "provided-dependencies")

    apply(plugin = rootProject.libs.plugins.shadow.get().pluginId)
    apply(plugin = rootProject.libs.plugins.gitProperties.get().pluginId)
    apply(plugin = "dev.vankka.dependencydownload.plugin")

    // Apply the shared build logic from a convention plugin.
    // The shared code is located in `buildSrc/src/main/kotlin/kotlin-jvm.gradle.kts`.
    apply(plugin = "buildsrc.convention.kotlin-jvm")

    dependencies {
        compileOnly(kotlin("stdlib"))
    }

    kotlin {
        jvmToolchain(21)
        compilerOptions {
            jvmTarget.set(JvmTarget.JVM_21)
        }
    }

    configure<com.gorylenko.GitPropertiesPluginExtension> {
        gitPropertiesName = "git.properties"
        keys = listOf(
            "git.branch",
            "git.build.time",
            "git.build.version",
            "git.commit.id.abbrev",
            "git.commit.id",
            "git.commit.time",
            "git.tags",
            "git.closest.tag.name",
            "git.closest.tag.commit.count"
        )

        dateFormat = "yyyy-MM-dd'T'HH:mm:ssZ"
        dateFormatTimeZone = "UTC"
        extProperty = "git"
    }

    tasks.withType<GenerateGitPropertiesTask> {
        outputs.upToDateWhen { false }
    }

    val git = project.extra["git"] as Map<*, *>
    val commitId = git["git.commit.id.abbrev"].toString()
    val commitIdFull = git["git.commit.id"].toString()
    val branchName = git["git.branch"].toString()

    val versionWithGit = "${project.version}-$commitId-$branchName"

    tasks {
        build {
            dependsOn(shadowJar)
        }

        compileJava {
            options.encoding = "UTF-8"
        }

        compileKotlin {

        }

        val rootName = rootProject.name
        val projName = project.name
        val projVersion = project.version.toString()
        val targetDir = rootProject.layout.projectDirectory.dir(".built")
        val archiveFile = shadowJar.flatMap { it.archiveFile }

        register<Copy>("copyToRootTarget") {
            from(archiveFile)
            into(targetDir)

            // Rename the file during the copy process
            rename { "${rootName}-${projName}-${projVersion}.jar" }
            dependsOn(shadowJar)
        }

        assemble {
            finalizedBy("copyToRootTarget")
        }

        val generateDependencies = register<GenerateDependencyDownloadResourceTask>("generateDependencies") {
            configuration.set(configurations.getByName("runtimeDownload"))
            includeShadowJarRelocations.set(true)
            hashingAlgorithm.set("SHA-256")
            file.set("dependencies.txt")
        }

        jar {
            dependsOn(generateDependencies)
        }

        shadowJar {
            dependsOn(generateDependencies)
        }
    }

    publishing {
        repositories.configureScalaRepository(dev = branchName != "main")
        publications {
            create<MavenPublication>("maven") {
                from(components["java"])
            }
        }
    }
}


fun RepositoryHandler.configureScalaRepository(dev: Boolean = false) {
    maven("${property("artifactory_contextUrl")}/gradle-${if (dev) "dev" else "release"}") {
        name = "scala"
        credentials {
            username = property("artifactory_user").toString()
            password = property("artifactory_password").toString()
        }
    }
}
