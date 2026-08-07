// https://docs.gradle.org/current/userguide/settings_file_basics.html
// Learn more about structuring projects with Gradle - https://docs.gradle.org/8.7/userguide/multi_project_builds.html

dependencyResolutionManagement {
    // Use Maven Central as the default repository (where Gradle will download dependencies) in all subprojects.
    @Suppress("UnstableApiUsage")
    repositories {
        gradlePluginPortal()
        mavenCentral()
        maven("https://repo.papermc.io/repository/maven-public/")
    }
}

pluginManagement {
    repositories {
        gradlePluginPortal()
        maven("https://repo.papermc.io/repository/maven-public/")
    }
}

plugins {
    // Use the Foojay Toolchains plugin to automatically download JDKs required by subprojects.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "universe"

registerSubProjects(
    root = "extensions",
    prefix = "extension",
    subProjects = arrayOf("api", "example", "runtime-docker", "runtime-k8s", "storage-s3", "db-postgres", "db-mongodb", "db-redis", "metrics-prometheus", "metrics-influxdb", "gitops", "argocd", "discord", "tailscale", "deployment-artifacts")
)

include(":loader")
include(":app")
include(":api")

registerSubProjects(
    root = "minecraft",
    prefix = "minecraft",
    subProjects = arrayOf("api", "modern", "legacy", "velocity", "folia", "bungee")
)

//include(":minecraft:modern")
//include(":minecraft:legacy")
//include(":minecraft:velocity")
//
private fun registerSubProjects(root: String, prefix: String? = null, vararg subProjects: String) {
    val subProjectNamePrefix = if (prefix.isNullOrBlank()) "" else "$prefix-"
    subProjects.forEach {
        val projectPath = "$root:$it"
        include(projectPath)
        project(":$projectPath").name = "$subProjectNamePrefix$it"
    }
}
