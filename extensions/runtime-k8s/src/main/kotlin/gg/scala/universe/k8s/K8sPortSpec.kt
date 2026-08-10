package gg.scala.universe.k8s

import gg.scala.universe.schema.AdditionalPort
import io.fabric8.kubernetes.api.model.ContainerPort
import io.fabric8.kubernetes.api.model.ContainerPortBuilder

/**
 * Builds host-port declarations for Kubernetes instance containers.
 *
 * A non-null `hostIP` makes CNI emit a destination-scoped DNAT rule using
 * `-d <hostIP>/32`.
 */
internal object K8sPortSpec {

    fun resolveHostIp(configured: String?): String? = configured
        ?.trim()
        ?.takeIf { it.isNotEmpty() && it != "0.0.0.0" }

    fun build(
        primaryPort: Int,
        additionalPorts: List<AdditionalPort>,
        configuredBindAddress: String?
    ): List<ContainerPort> {
        val hostIp = resolveHostIp(configuredBindAddress)
        return buildList {
            add(port(primaryPort, "TCP", null, hostIp))
            additionalPorts.forEach { additional ->
                val protocol = if (additional.protocol.equals("udp", true)) "UDP" else "TCP"
                add(port(additional.port, protocol, additional.name.ifBlank { "port-${additional.port}" }, hostIp))
            }
        }
    }

    private fun port(port: Int, protocol: String, name: String?, hostIp: String?): ContainerPort =
        ContainerPortBuilder()
            .withContainerPort(port)
            .withHostPort(port)
            .withProtocol(protocol)
            .withName(name)
            .withHostIP(hostIp)
            .build()
}
