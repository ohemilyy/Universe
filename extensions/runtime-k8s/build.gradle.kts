dependencies {
    compileOnly(project(":api"))
    compileOnly(project(":extensions:extension-api"))
    runtimeDownload(libs.bundles.k8s)

    testImplementation(libs.kotlin.test.junit5)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(project(":api"))
    testImplementation(libs.k8s.client)
    testImplementation(libs.k8s.server.mock)
}
