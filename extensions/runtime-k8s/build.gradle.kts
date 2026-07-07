dependencies {
    compileOnly(project(":api"))
    compileOnly(project(":extensions:extension-api"))
    runtimeDownload(libs.bundles.k8s)

    testImplementation(project(":api"))
    testImplementation(libs.k8s.client)
    testImplementation(libs.k8s.server.mock)
    testImplementation(libs.k8s.junit)
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}
