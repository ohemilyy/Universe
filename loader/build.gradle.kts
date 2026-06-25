plugins {
    application
    alias(libs.plugins.jib)
}

application {
    mainClass.set("LoaderKt")
}

dependencies {
    implementation(libs.dependencyDownload)
    implementation("me.lucko:jar-relocator:1.7")
    implementation("org.ow2.asm:asm:9.10")
    implementation("org.ow2.asm:asm-commons:9.10.1")

    implementation(kotlin("stdlib"))
    implementation(kotlin("reflect"))
}

tasks {
    shadowJar {
//        exclude("**/*.kotlin_metadata")
        archiveFileName.set("universe.jar")

        project(":app")?.let {
            dependsOn(it.tasks.shadowJar)
        }

        from(project(":app").tasks.shadowJar)
    }
}

tasks.jib {
    dependsOn(tasks.shadowJar)
}

// ── Jib configuration ─────────────────────────────────────────
// For CI (ghcr.io): set JIB_IMAGE, JIB_USERNAME, JIB_PASSWORD, JIB_TAGS env vars
// For local dev: uses default git.lunarlabs.dev registry
jib {
    from {
        image = "amazoncorretto:21"
    }
    to {
        image = System.getenv("JIB_IMAGE") ?: "git.lunarlabs.dev/scala/universe:dev"
        setTags(provider {
            val envTags = System.getenv("JIB_TAGS")
            if (envTags != null) {
                envTags.split(",").map { it.trim() }.toSet()
            } else {
                val git = project.extra["git"] as Map<*, *>
                val branchName = git["git.branch"].toString()
                setOf("dev", branchName, "latest")
            }
        })

        auth {
            username = System.getenv("JIB_USERNAME") ?: System.getenv("REGISTRY_USER") ?: ""
            password = System.getenv("JIB_PASSWORD") ?: System.getenv("REGISTRY_PASS") ?: ""
        }
    }
    container {
        entrypoint = listOf(
            "java",
            "--add-opens=java.base/java.time=ALL-UNNAMED",
            "--add-opens=java.base/java.net=ALL-UNNAMED",
            "--add-opens=java.base/java.io=ALL-UNNAMED",
            "--enable-native-access=ALL-UNNAMED",
            "--add-modules=java.se",
            "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-exports=java.base/sun.misc=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens=java.management/sun.management=ALL-UNNAMED",
            "--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED",
            "-jar", "/app/universe.jar"
        )

        creationTime = provider {
            val git = project.extra["git"] as Map<*, *>
            git["git.commit.time"].toString()
        }

        workingDirectory = "/data"

        labels = mapOf(
            "org.opencontainers.image.source" to (
                System.getenv("GITHUB_REPOSITORY")
                    ?.let { "https://github.com/$it" }
                    ?: "https://git.lunarlabs.dev/scala/universe"
                ),
            "org.opencontainers.image.description" to "Universe — Distributed single-JAR cluster orchestrator",
            "org.opencontainers.image.licenses" to "UNLICENSED"
        )
    }
    extraDirectories {
        paths {
            path {
                setFrom(tasks.shadowJar.get().archiveFile.get().asFile.parentFile.toPath())
                into = "/app"
                includes = listOf(tasks.shadowJar.get().archiveFileName.get())
            }
        }
    }
}
