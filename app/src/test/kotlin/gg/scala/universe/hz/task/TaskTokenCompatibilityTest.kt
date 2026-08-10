package gg.scala.universe.hz.task

import gg.scala.universe.task.DeployInstanceTask
import gg.scala.universe.task.StopInstanceTask
import gg.scala.universe.util.json.Serializers
import kotlin.test.Test
import kotlin.test.assertEquals

class TaskTokenCompatibilityTest {
    @Test
    fun `legacy task payloads deserialize with generation zero`() {
        val deploy = Serializers.GSON.fromJson(
            """{"instanceId":"abc123","configurationName":"site","type":"deploy"}""",
            DeployInstanceTask::class.java
        )
        val stop = Serializers.GSON.fromJson(
            """{"instanceId":"abc123","force":true,"restart":false,"type":"stop"}""",
            StopInstanceTask::class.java
        )

        assertEquals(0L, deploy.expectedGeneration)
        assertEquals(0L, stop.expectedGeneration)
    }
}
