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

jib {
    from {
        image = "amazoncorretto:21"
    }
    to {
        image = "git.lunarlabs.dev/scala/universe:dev"
        setTags(provider {
            val git = project.extra["git"] as Map<*, *>
//            val commitIdFull = git["git.commit.id"].toString()
            val branchName = git["git.branch"].toString()

            setOf("dev", /* commitIdFull, */ branchName, "latest")
        })

        auth {
            username = System.getenv("REGISTRY_USER") ?: ""
            password = System.getenv("REGISTRY_PASS") ?: ""
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
            // Retrieve the 'git' extra property and cast it to a Map
            val git = project.extra["git"] as Map<*, *>
            git["git.commit.time"].toString()
        }

        workingDirectory = "/data"

        labels = mapOf(
            "org.opencontainers.image.source" to "https://git.lunarlabs.dev/scala/universe"
        )

        // Tell Jib to copy the ShadowJar into the /app directory of the image
        extraDirectories {
            paths {
                path {
                    setFrom(tasks.shadowJar.get().archiveFile.get().asFile.parentFile)
                    into = "/app"
                    includes = listOf(tasks.shadowJar.get().archiveFileName.get())
                }
            }
        }
    }
}