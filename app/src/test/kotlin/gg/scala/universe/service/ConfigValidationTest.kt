package gg.scala.universe.service

import gg.scala.universe.schema.Configuration
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ConfigValidationTest {

    @Test
    fun `warns when a proxy entry point has minimum zero`() {
        val config = Configuration(name = "legacy-proxy", runtime = "kube", minimumServiceCount = 0)
        val warnings = ConfigValidation.warnings(config)
        assertTrue(warnings.any { it.contains("minimumServiceCount") }, warnings.toString())
    }

    @Test
    fun `warns when the entrypoint property is set and minimum is zero`() {
        val config = Configuration(
            name = "network-gateway",
            minimumServiceCount = 0,
            properties = mapOf("entrypoint" to "true")
        )
        assertTrue(ConfigValidation.warnings(config).isNotEmpty())
    }

    @Test
    fun `does not warn for a normal on-demand service at minimum zero`() {
        val config = Configuration(name = "minigame-arena", runtime = "kube", minimumServiceCount = 0)
        assertTrue(ConfigValidation.warnings(config).isEmpty())
    }

    @Test
    fun `does not warn for a proxy that is correctly always-on`() {
        val config = Configuration(name = "velocity-proxy", minimumServiceCount = 1)
        assertTrue(ConfigValidation.warnings(config).isEmpty())
    }

    @Test
    fun `does not warn for a static proxy`() {
        val config = Configuration(name = "proxy", static = true, minimumServiceCount = 0)
        assertFalse(ConfigValidation.warnings(config).any())
    }
}
