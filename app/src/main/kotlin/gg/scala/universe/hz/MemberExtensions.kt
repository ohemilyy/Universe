package gg.scala.universe.hz

import com.hazelcast.cluster.Member

/**
 * Returns the human-readable node name for a Hazelcast member.
 * Falls back to the UUID if no nodeId attribute is set.
 */
fun Member.nodeName(): String {
    return this.getAttribute("nodeId") ?: this.uuid.toString()
}

/**
 * Returns the STABLE identity used for instance ownership and node-resource accounting.
 *
 * Unlike the Hazelcast member UUID, the configured `nodeId` attribute survives JVM restarts,
 * so ownership keyed on it is not orphaned when a node reconnects with a fresh UUID. Falls
 * back to the UUID only when no nodeId attribute is configured.
 */
fun Member.stableNodeId(): String {
    return this.getAttribute("nodeId") ?: this.uuid.toString()
}

/**
 * True if this member is the one referenced by [storedNodeId]. Matches either the stable
 * nodeId (current scheme) or the raw UUID (legacy records created before the re-key, or
 * still in flight within the same session), so the comparison is robust both ways.
 */
fun Member.ownsInstance(storedNodeId: String): Boolean {
    return this.getAttribute("nodeId") == storedNodeId || this.uuid.toString() == storedNodeId
}
