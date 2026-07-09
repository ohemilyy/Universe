package gg.scala.universe.runtime

/**
 * Pure helpers for identifying and scoping ownership of a runtime's external resources
 * (e.g. Kubernetes Pods/Services) from their tags/labels, with a backward-compatible fallback
 * to a legacy naming scheme. Kept free of any runtime dependency so the logic is unit-testable
 * in isolation, mirroring [RuntimeReconcile].
 *
 * @author Luna
 * @date 2026-06-25
 */
object ResourceOwnership {

    /**
     * Resolves the Universe instance id for a resource. Prefers [idKey] in [tags], then the
     * deprecated [legacyIdKey], and finally parses a legacy `<legacyNamePrefix><id>` resource
     * [name] so resources created before the label scheme existed can still be matched and
     * cleaned up. Returns null when none apply.
     */
    fun instanceId(
        tags: Map<String, String>,
        name: String?,
        idKey: String,
        legacyIdKey: String,
        legacyNamePrefix: String
    ): String? {
        tags[idKey]?.takeIf { it.isNotBlank() }?.let { return it }
        tags[legacyIdKey]?.takeIf { it.isNotBlank() }?.let { return it }
        if (name != null && legacyNamePrefix.isNotBlank() && name.startsWith(legacyNamePrefix)) {
            val id = name.removePrefix(legacyNamePrefix)
            if (id.isNotBlank()) return id
        }
        return null
    }

    /**
     * Whether a resource is in scope for the cluster [ourCluster] to manage. A resource whose
     * [clusterKey] tag names a *different* cluster is never ours (so two Universe clusters can
     * safely share a namespace). An untagged (legacy) resource is treated as ours so it can
     * still be reconciled. When [ourCluster] is blank, ownership scoping is disabled.
     */
    fun belongsToCluster(tags: Map<String, String>, clusterKey: String, ourCluster: String): Boolean {
        if (ourCluster.isBlank()) return true
        val tag = tags[clusterKey] ?: return true
        return tag == ourCluster
    }
}
