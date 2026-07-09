package gg.scala.universe.runtime

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ResourceOwnershipTest {

    private val idKey = "universe.instance"
    private val legacyIdKey = "universe-instance-id"
    private val prefix = "universe-"

    @Test
    fun `prefers the modern id label`() {
        val id = ResourceOwnership.instanceId(
            mapOf(idKey to "abc123", legacyIdKey to "old999"),
            name = "universe-name000",
            idKey = idKey, legacyIdKey = legacyIdKey, legacyNamePrefix = prefix
        )
        assertEquals("abc123", id)
    }

    @Test
    fun `falls back to the legacy id label`() {
        val id = ResourceOwnership.instanceId(
            mapOf(legacyIdKey to "old999"),
            name = "universe-name000",
            idKey = idKey, legacyIdKey = legacyIdKey, legacyNamePrefix = prefix
        )
        assertEquals("old999", id)
    }

    @Test
    fun `parses the legacy resource name when no labels are present`() {
        val id = ResourceOwnership.instanceId(
            emptyMap(),
            name = "universe-cc100a",
            idKey = idKey, legacyIdKey = legacyIdKey, legacyNamePrefix = prefix
        )
        assertEquals("cc100a", id)
    }

    @Test
    fun `returns null when nothing identifies the resource`() {
        val id = ResourceOwnership.instanceId(
            emptyMap(),
            name = "some-unrelated-pod",
            idKey = idKey, legacyIdKey = legacyIdKey, legacyNamePrefix = prefix
        )
        assertNull(id)
    }

    @Test
    fun `same-cluster resource is owned`() {
        assertTrue(
            ResourceOwnership.belongsToCluster(mapOf("universe.cluster" to "scala-prod"), "universe.cluster", "scala-prod")
        )
    }

    @Test
    fun `different-cluster resource is not owned`() {
        assertFalse(
            ResourceOwnership.belongsToCluster(mapOf("universe.cluster" to "other"), "universe.cluster", "scala-prod")
        )
    }

    @Test
    fun `legacy resource without a cluster label is treated as owned`() {
        // Pre-existing resources carry no cluster label and must remain reconcilable.
        assertTrue(
            ResourceOwnership.belongsToCluster(emptyMap(), "universe.cluster", "scala-prod")
        )
    }

    @Test
    fun `blank own-cluster disables scoping`() {
        assertTrue(
            ResourceOwnership.belongsToCluster(mapOf("universe.cluster" to "anything"), "universe.cluster", "")
        )
    }
}
