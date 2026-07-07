package gg.scala.universe.runtime

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class JvmHeapArgsTest {

    @Test
    fun `sizes heap to the configured fraction of ramMB`() {
        val out = JvmHeapArgs.inject("java -jar paper.jar nogui", ramMB = 8192, fraction = 0.75)
        assertEquals("java -Xms6144M -Xmx6144M -jar paper.jar nogui", out)
    }

    @Test
    fun `does not override an explicit -Xmx`() {
        val cmd = "java -Xmx2G -jar paper.jar"
        assertEquals(cmd, JvmHeapArgs.inject(cmd, ramMB = 16384, fraction = 0.75))
    }

    @Test
    fun `does not override an explicit -Xms`() {
        val cmd = "java -Xms1G -jar paper.jar"
        assertEquals(cmd, JvmHeapArgs.inject(cmd, ramMB = 16384, fraction = 0.75))
    }

    @Test
    fun `leaves a non-java command untouched`() {
        val cmd = "./bootstrap.sh --serve"
        assertEquals(cmd, JvmHeapArgs.inject(cmd, ramMB = 8192, fraction = 0.75))
    }

    @Test
    fun `does not match a lookalike token`() {
        val cmd = "run-javashim --go"
        assertEquals(cmd, JvmHeapArgs.inject(cmd, ramMB = 8192, fraction = 0.75))
    }

    @Test
    fun `injects after an absolute java path`() {
        val out = JvmHeapArgs.inject("/usr/bin/java -jar x.jar", ramMB = 4096, fraction = 0.5)
        assertEquals("/usr/bin/java -Xms2048M -Xmx2048M -jar x.jar", out)
    }

    @Test
    fun `injects after java even when preceded by a shell prefix`() {
        val out = JvmHeapArgs.inject("cd /app && java -jar x.jar", ramMB = 2048, fraction = 0.75)
        assertTrue(out.contains("java -Xms1536M -Xmx1536M -jar x.jar"), out)
    }

    @Test
    fun `does not inject when the floor would meet or exceed the container`() {
        val cmd = "java -jar x.jar"
        assertEquals(cmd, JvmHeapArgs.inject(cmd, ramMB = 256, fraction = 0.75, minHeapMB = 512))
    }

    @Test
    fun `does not inject when the sized heap would leave no headroom`() {
        val cmd = "java -jar x.jar"
        assertEquals(cmd, JvmHeapArgs.inject(cmd, ramMB = 512, fraction = 0.75, minHeapMB = 512))
    }

    @Test
    fun `does not override container-aware -XX MaxRAMPercentage`() {
        val cmd = "java -XX:MaxRAMPercentage=90.0 -XX:InitialRAMPercentage=75.0 -jar server.jar"
        assertEquals(cmd, JvmHeapArgs.inject(cmd, ramMB = 8192, fraction = 0.75))
    }

    @Test
    fun `does not mistake an inline JAVA_HOME assignment for the launcher`() {
        val out = JvmHeapArgs.inject("JAVA_HOME=/opt/java java -jar x.jar", ramMB = 4096, fraction = 0.5)
        assertEquals("JAVA_HOME=/opt/java java -Xms2048M -Xmx2048M -jar x.jar", out)
    }

    @Test
    fun `returns the command unchanged when ramMB is not positive`() {
        val cmd = "java -jar x.jar"
        assertEquals(cmd, JvmHeapArgs.inject(cmd, ramMB = 0, fraction = 0.75))
    }

    @Test
    fun `safeHeapMB reports the size inject would choose, or null when unsafe`() {
        assertEquals(6144, JvmHeapArgs.safeHeapMB(8192, 0.75))
        assertEquals(512, JvmHeapArgs.safeHeapMB(683, 0.75, 512))
        assertEquals(null, JvmHeapArgs.safeHeapMB(256, 0.75, 512))
        assertEquals(null, JvmHeapArgs.safeHeapMB(512, 0.75, 512))
    }
}
