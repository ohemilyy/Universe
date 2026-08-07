dependencies {
    implementation(project(":extensions:extension-api"))

    // JDA for Discord bot
    runtimeDownload(libs.jda)

    // Cloud JDA6 commands
    runtimeDownload(libs.cloud.jda6)
    runtimeDownload(libs.cloudAnnotations)

    // Shade Gson (relocated)
    implementation(libs.gson)
}

tasks {
    shadowJar {
        archiveClassifier.set("")
        archiveVersion.set("")
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
