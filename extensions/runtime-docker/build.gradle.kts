dependencies {
    runtimeDownload(libs.bundles.dockerJava)

    testImplementation(libs.kotlin.test.junit5)
    testImplementation(project(":api"))
    testImplementation(libs.bundles.dockerJava)
    testRuntimeOnly(libs.junit.jupiter.engine)
}
