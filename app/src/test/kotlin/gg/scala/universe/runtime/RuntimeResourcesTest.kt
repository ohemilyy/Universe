package gg.scala.universe.runtime

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class RuntimeResourcesTest {

    @Test
    fun `100 cpu units is one core in millicores`() {
        assertEquals(1000, RuntimeResources.cpuUnitsToMillicores(100))
    }

    @Test
    fun `800 cpu units is eight cores not 800 cores`() {
        assertEquals(8000, RuntimeResources.cpuUnitsToMillicores(800))
    }

    @Test
    fun `zero cpu units converts to zero`() {
        assertEquals(0, RuntimeResources.cpuUnitsToMillicores(0))
    }

    @Test
    fun `small request fits a large idle node`() {
        // 128GB / 16-core box, nothing scheduled, asking for 2GB / 1 core.
        assertTrue(
            RuntimeResources.fits(
                allocatableCpuMillis = 16_000,
                allocatableMemMB = 131_072,
                usedCpuMillis = 0,
                usedMemMB = 0,
                requestCpuMillis = 1000,
                requestMemMB = 2048
            )
        )
    }

    @Test
    fun `request exactly filling remaining capacity fits`() {
        assertTrue(
            RuntimeResources.fits(
                allocatableCpuMillis = 4000,
                allocatableMemMB = 8192,
                usedCpuMillis = 3000,
                usedMemMB = 6144,
                requestCpuMillis = 1000,
                requestMemMB = 2048
            )
        )
    }

    @Test
    fun `request over remaining cpu does not fit`() {
        assertFalse(
            RuntimeResources.fits(
                allocatableCpuMillis = 4000,
                allocatableMemMB = 8192,
                usedCpuMillis = 3500,
                usedMemMB = 0,
                requestCpuMillis = 1000,
                requestMemMB = 2048
            )
        )
    }

    @Test
    fun `request over remaining memory does not fit`() {
        assertFalse(
            RuntimeResources.fits(
                allocatableCpuMillis = 16_000,
                allocatableMemMB = 8192,
                usedCpuMillis = 0,
                usedMemMB = 7000,
                requestCpuMillis = 1000,
                requestMemMB = 2048
            )
        )
    }
}
