dependencies {
    compileOnly(libs.folia.api)
    implementation(project(":minecraft:minecraft-api"))
    implementation(kotlin("stdlib"))

    // Cloud commands
    implementation(libs.cloud.paper)
    implementation(libs.cloudAnnotations)

    // Shade Gson for JSON serialization (relocated)
    implementation(libs.gson)
}

tasks {
    processResources {
        filesMatching("plugin.yml") {
            expand(mapOf("version" to project.version))
        }
    }

    shadowJar {
        archiveClassifier.set("")
        archiveVersion.set("")

        // Relocate shaded libraries (not the api module)
        relocate("com.google.gson", "gg.scala.universe.libs.gson")
        relocate("org.incendo.cloud", "gg.scala.universe.libs.cloud")
        relocate("kotlin", "gg.scala.universe.libs.kotlin") {
            exclude("kotlin/Metadata.class")
            exclude("kotlin/metadata/**")
        }
        relocate("kotlinx", "gg.scala.universe.libs.kotlinx")
    }

    build {
        dependsOn(shadowJar)
    }
}

kotlin {
    compilerOptions {
        jvmTarget = org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21
    }
}
