package gg.scala.universe.cluster

import gg.scala.universe.schema.Configuration
import gg.scala.universe.schema.InstanceInfo

/**
 * Read-only access to cluster state data for extensions.
 *
 * Extensions can inject this interface to query configurations and instances
 * without depending on the :app module.
 */
interface ClusterDataProvider {
    /** Returns all known configurations. */
    fun getConfigurations(): Collection<Configuration>

    /** Returns all active (non-stopped) instances. */
    fun getActiveInstances(): Collection<InstanceInfo>

    /** Returns all instances including stopped/offline. */
    fun getAllInstances(): Collection<InstanceInfo>

    /** The logical cluster name this node belongs to. Stable across restarts. */
    fun getClusterName(): String

    /** The stable logical id of the local node. Stable across restarts (not the Hazelcast UUID). */
    fun getLocalNodeId(): String
}
