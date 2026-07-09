package gg.scala.universe.cluster

import com.google.inject.Inject
import com.google.inject.Singleton
import gg.scala.universe.config.UniverseMainConfiguration
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.schema.Configuration
import gg.scala.universe.schema.InstanceInfo

/**
 * App-module implementation of [ClusterDataProvider] backed by [ClusterStateService].
 */
@Singleton
class ClusterDataProviderImpl @Inject constructor(
    private val clusterStateService: ClusterStateService,
    private val configuration: UniverseMainConfiguration
) : ClusterDataProvider {

    override fun getConfigurations(): Collection<Configuration> {
        return clusterStateService.configurations.values
    }

    override fun getActiveInstances(): Collection<InstanceInfo> {
        return clusterStateService.getActiveInstances()
    }

    override fun getAllInstances(): Collection<InstanceInfo> {
        return clusterStateService.getAllInstances()
    }

    override fun getClusterName(): String = configuration.clusterName

    override fun getLocalNodeId(): String = configuration.nodeId
}
