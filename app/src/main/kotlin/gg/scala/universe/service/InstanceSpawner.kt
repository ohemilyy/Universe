package gg.scala.universe.service

import gg.scala.universe.schema.Configuration
import gg.scala.universe.schema.InstanceInfo

interface InstanceSpawner {
    fun createInstance(configuration: Configuration, instanceId: String? = null): InstanceInfo?
}
