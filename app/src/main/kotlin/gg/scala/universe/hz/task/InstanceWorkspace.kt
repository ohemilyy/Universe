package gg.scala.universe.hz.task

import com.google.inject.Inject
import com.google.inject.Singleton
import java.nio.file.Path
import java.nio.file.Paths

/** Owns the filesystem roots used for instance state and working data. */
@Singleton
class InstanceWorkspace private constructor(private val root: Path) {
    @Inject
    constructor() : this(Paths.get(".").toAbsolutePath().normalize())

    internal constructor(testRoot: Path, normalize: Boolean = true) : this(
        if (normalize) testRoot.toAbsolutePath().normalize() else testRoot
    )

    fun runningRoot(): Path = root.resolve("running").normalize()
    fun staticRoot(): Path = root.resolve("static").normalize()
    fun dynamicInstance(instanceId: String): Path = child(runningRoot(), instanceId)
    fun staticConfiguration(configurationName: String): Path = child(staticRoot(), configurationName)

    private fun child(parent: Path, name: String): Path {
        val resolved = parent.resolve(name).normalize()
        require(resolved.parent == parent) { "Unsafe workspace child '$name'" }
        return resolved
    }
}
