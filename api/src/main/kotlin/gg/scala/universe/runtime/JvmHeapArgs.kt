package gg.scala.universe.runtime

object JvmHeapArgs {

    private val JAVA_TOKEN = Regex("""(^|\s)((?:[^\s=]*/)?java)(\s)""")

    private val EXPLICIT_HEAP_FLAGS = listOf(
        "-Xmx", "-Xms", "-Xmn",
        "-XX:MaxHeapSize", "-XX:MinHeapSize", "-XX:InitialHeapSize",
        "-XX:MaxRAMPercentage", "-XX:MinRAMPercentage", "-XX:InitialRAMPercentage",
        "-XX:MaxRAMFraction"
    )

    fun inject(command: String, ramMB: Int, fraction: Double = 0.75, minHeapMB: Int = 512): String {
        if (ramMB <= 0 || fraction <= 0.0) return command
        if (EXPLICIT_HEAP_FLAGS.any { command.contains(it) }) return command
        val heap = safeHeapMB(ramMB, fraction, minHeapMB) ?: return command
        val match = JAVA_TOKEN.find(command) ?: return command

        val flags = "-Xms${heap}M -Xmx${heap}M"
        val replacement = "${match.groupValues[1]}${match.groupValues[2]} $flags${match.groupValues[3]}"
        return command.replaceRange(match.range, replacement)
    }

    fun safeHeapMB(ramMB: Int, fraction: Double = 0.75, minHeapMB: Int = 512): Int? {
        val heap = maxOf(minHeapMB, (ramMB * fraction).toInt())
        return if (heap < ramMB) heap else null
    }
}
