package gg.scala.universe.service

import gg.scala.universe.schema.Configuration

object ConfigValidation {

    private val ENTRY_POINT_HINTS = listOf("proxy", "velocity", "bungee", "waterfall", "gate")

    fun warnings(config: Configuration): List<String> {
        val out = mutableListOf<String>()
        if (config.minimumServiceCount <= 0 && !config.static && looksLikeEntryPoint(config)) {
            out += "Configuration '${config.name}' looks like an always-on entry point " +
                "(proxy/gateway) but has minimumServiceCount=${config.minimumServiceCount}; it will " +
                "never be auto-spawned. Once its last instance stops, nothing restarts it and players " +
                "cannot connect. Set minimumServiceCount >= 1."
        }
        return out
    }

    private fun looksLikeEntryPoint(config: Configuration): Boolean {
        if (config.properties["entrypoint"].equals("true", ignoreCase = true)) return true
        val haystack = buildString {
            append(config.name).append(' ')
            append(config.command).append(' ')
            append(config.instanceGroups.joinToString(" "))
        }.lowercase()
        return ENTRY_POINT_HINTS.any { haystack.contains(it) }
    }
}
