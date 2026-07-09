package gg.scala.universe.runtime

/**
 * Pure resource-unit conversions and scheduling-fit checks shared between the
 * scheduler and the runtime providers.
 *
 * Universe expresses CPU in "units" where **100 units == 1 core**. Kubernetes expresses
 * CPU in millicores where **1000m == 1 core**. Therefore the only correct conversion is
 * `units * 10` (100 units -> 1000m -> 1 core). This is deliberately centralised and
 * tested so the conversion can never silently drift into impossible requests like
 * "100 cores".
 *
 * @author Luna
 * @date 2026-06-25
 */
/**
 * Schedulable capacity for a node as reported by a runtime, in Kubernetes-native units
 * (CPU millicores, memory MB). [usedCpuMillis]/[usedMemMB] reflect what live, non-terminated
 * resources already request — never raw real-time usage.
 */
data class NodeAllocatable(
    val cpuMillis: Long,
    val memMB: Long,
    val usedCpuMillis: Long = 0,
    val usedMemMB: Long = 0
)

object RuntimeResources {

    /** Converts Universe CPU units (100 == 1 core) to Kubernetes millicores (1000m == 1 core). */
    fun cpuUnitsToMillicores(cpuUnits: Int): Int {
        return cpuUnits * 10
    }

    /**
     * Returns true if a request fits a node given what is already requested by live
     * (non-terminated) resources. All CPU values are millicores; all memory values are MB.
     */
    fun fits(
        allocatableCpuMillis: Long,
        allocatableMemMB: Long,
        usedCpuMillis: Long,
        usedMemMB: Long,
        requestCpuMillis: Long,
        requestMemMB: Long
    ): Boolean {
        return usedCpuMillis + requestCpuMillis <= allocatableCpuMillis &&
            usedMemMB + requestMemMB <= allocatableMemMB
    }
}
