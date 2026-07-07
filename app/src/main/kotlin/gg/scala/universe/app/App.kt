package gg.scala.universe.app

import com.google.inject.Guice
import com.google.inject.Injector
import com.google.inject.Module
import com.hazelcast.core.HazelcastInstance
import gg.scala.universe.console.Ansi
import gg.scala.universe.console.Console
import gg.scala.universe.console.log
import org.slf4j.LoggerFactory
import gg.scala.universe.api.ApiGuiceModule
import gg.scala.universe.api.KtorServerService
import gg.scala.universe.config.ConfigLoader
import gg.scala.universe.config.ConfigurationLoader
import gg.scala.universe.config.UniverseMainConfiguration
import gg.scala.universe.extension.ExtensionService
import gg.scala.universe.hz.ClusterStateService
import gg.scala.universe.hz.HazelcastService
import gg.scala.universe.hz.HzGuiceModule
import gg.scala.universe.hz.ResilienceMembershipListener
import gg.scala.universe.command.CommandBootstrap
import gg.scala.universe.console.LogLevel
import gg.scala.universe.runtime.RuntimeRegistry
import gg.scala.universe.runtime.ProcessRuntimeProvider
import gg.scala.universe.runtime.ScreenRuntimeProvider
import gg.scala.universe.runtime.TmuxRuntimeProvider
import gg.scala.universe.service.AutoUpdaterService
import gg.scala.universe.service.InstanceCountEnforcer
import gg.scala.universe.service.InstanceHealthMonitor
import gg.scala.universe.service.InstanceRecoveryService
import gg.scala.universe.service.NodeShutdownService

fun run() {
    log("Starting Universe")
    UniverseApplication()
}

class UniverseApplication {
    private val logger = LoggerFactory.getLogger(UniverseApplication::class.java)

    var mainConfiguration: UniverseMainConfiguration

    val injector: Injector
    val hzService: HazelcastService
    val extensionService: ExtensionService
    var commandBootstrap: CommandBootstrap

    @Volatile
    private var isShuttingDown = false

    init {
        instance = this
        mainConfiguration = ConfigLoader.load()

        Console.setDebug(mainConfiguration.debug)
        configureLogbackLevels(mainConfiguration.debug)

        injector = Guice.createInjector(guiceModules)

        hzService = injector.getInstance(HazelcastService::class.java)

        // Register built-in runtime providers
        val runtimeRegistry = injector.getInstance(RuntimeRegistry::class.java)
        runtimeRegistry.register("tmux", TmuxRuntimeProvider())
        runtimeRegistry.register("screen", ScreenRuntimeProvider())
        runtimeRegistry.register("process", ProcessRuntimeProvider())
        log("Registered built-in runtime providers (tmux, screen, process)")

        // Load extensions BEFORE starting services that process Hazelcast tasks.
        // This prevents "Runtime 'kube' not registered" errors when deploy
        // tasks arrive from the master immediately after joining.
        extensionService = ExtensionService()
        injector.injectMembers(extensionService)
        extensionService.installExtensions()
        extensionService.loadExtensions()

        // Start health monitor on every node (wrappers run instances too)
        val healthMonitor = injector.getInstance(InstanceHealthMonitor::class.java)
        healthMonitor.start()

        // Start auto-updater for remote configurations and templates
        val autoUpdater = injector.getInstance(AutoUpdaterService::class.java)
        autoUpdater.start()

        if (mainConfiguration.isMasterNode) {
            val clusterStateService = injector.getInstance(ClusterStateService::class.java)
            hzService.hzInstance.cluster.addMembershipListener(
                ResilienceMembershipListener(clusterStateService)
            )
            log("Registered MembershipListener for instance resilience")
            ConfigurationLoader.load(clusterStateService)
        }

        // Recover instances that were running before restart
        val recoveryService = injector.getInstance(InstanceRecoveryService::class.java)
        recoveryService.recover()

        commandBootstrap = injector.getInstance(CommandBootstrap::class.java)
        commandBootstrap.start()

        // Register explicit signal handlers so Docker SIGTERM (and Ctrl+C)
        // triggers the same graceful shutdown as the CLI `stop` command.
        registerSignalHandlers()

        // Keep the shutdown hook as a fallback in case exit() is called
        // from a path that bypasses the signal handlers.
        Runtime.getRuntime().addShutdownHook(Thread({
            shutdown()
        }, "universe-shutdown"))

        if (mainConfiguration.isMasterNode) {
            val ktorService = injector.getInstance(KtorServerService::class.java)
            ktorService.start()

            val enforcer = injector.getInstance(InstanceCountEnforcer::class.java)
            enforcer.start()

            recoveryService.startPeriodicReconcile(60)
        }
    }

    /**
     * Performs a graceful shutdown of the entire node.
     * Called by the CLI `stop` command, signal handlers, and the JVM shutdown hook.
     * Guarded by [isShuttingDown] so it only executes once.
     */
    fun shutdown() {
        if (isShuttingDown) return
        synchronized(this) {
            if (isShuttingDown) return
            isShuttingDown = true
        }

        try {
            // Stop the console first so JLine terminal is closed cleanly
            // and stdout/stderr are restored to their originals.
            commandBootstrap.stop()

            // Use println directly to bypass JLine entirely during shutdown
            println("  ${Ansi.BLUE}→${Ansi.RESET} Shutting down Universe...")

            // Stop background services first so they don't race with Hazelcast
            println("  ${Ansi.BLUE}→${Ansi.RESET} Stopping background services...")
            val healthMonitor = injector.getInstance(InstanceHealthMonitor::class.java)
            healthMonitor.stop()
            val autoUpdater = injector.getInstance(AutoUpdaterService::class.java)
            autoUpdater.stop()

            // Stop the instance enforcer BEFORE stopping instances so it doesn't
            // see the count drop below minimum and auto-spawn new ones.
            if (mainConfiguration.isMasterNode) {
                val enforcer = injector.getInstance(InstanceCountEnforcer::class.java)
                enforcer.stop()
                injector.getInstance(InstanceRecoveryService::class.java).stopPeriodicReconcile()
            }

            // Stop local instances (needs Hazelcast)
            println("  ${Ansi.BLUE}→${Ansi.RESET} Stopping local instances...")
            val nodeShutdownService = injector.getInstance(NodeShutdownService::class.java)
            nodeShutdownService.stopAllLocalInstances()

            // Shutdown Hazelcast last
            println("  ${Ansi.BLUE}→${Ansi.RESET} Shutting down Hazelcast cluster...")
            try {
                hzService.hzInstance.shutdown()
            } catch (_: com.hazelcast.core.HazelcastInstanceNotActiveException) {
                // Already shut down
            }
            println("  ${Ansi.GREEN}✓${Ansi.RESET} Universe shutdown complete")
        } catch (e: Exception) {
            println("  ${Ansi.RED}✗${Ansi.RESET} Error during shutdown: ${e.message}")
            e.printStackTrace()
        }
    }

    /**
     * Registers signal handlers for SIGTERM (Docker `docker compose down`) and SIGINT (Ctrl+C)
     * so they trigger the same graceful shutdown path as the CLI `stop` command.
     */
    private fun registerSignalHandlers() {
        try {
            val signalClass = Class.forName("sun.misc.Signal")
            val handlerClass = Class.forName("sun.misc.SignalHandler")
            val handleMethod = signalClass.getMethod("handle", signalClass, handlerClass)

            val handler = java.lang.reflect.Proxy.newProxyInstance(
                handlerClass.classLoader,
                arrayOf(handlerClass)
            ) { _, _, _ ->
                shutdown()
                Runtime.getRuntime().exit(0)
                null
            }

            // SIGTERM — sent by Docker on `docker compose down`
            handleMethod.invoke(null, signalClass.getConstructor(String::class.java).newInstance("TERM"), handler)
            // SIGINT — Ctrl+C in attached terminal
            handleMethod.invoke(null, signalClass.getConstructor(String::class.java).newInstance("INT"), handler)
        } catch (_: Exception) {
            // Signal API not available — fallback to shutdown hook only
        }
    }

    companion object {
        @JvmField
        val guiceModules = mutableListOf<Module>(
            MainGuiceModule(), HzGuiceModule(), ApiGuiceModule()
        )

        lateinit var instance: UniverseApplication
    }
}

private fun configureLogbackLevels(debug: Boolean) {
    val loggerContext = LoggerFactory.getILoggerFactory() as? ch.qos.logback.classic.LoggerContext ?: return
    val root = loggerContext.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME)
    val appLogger = loggerContext.getLogger("gg.scala.universe")

    val frameworkLoggers = listOf(
        "com.hazelcast",
        "io.fabric8.kubernetes",
        "com.github.dockerjava",
        "org.apache.http",
        "software.amazon.awssdk",
        "io.netty"
    )

    if (debug) {
        root.level = ch.qos.logback.classic.Level.INFO
        appLogger.level = ch.qos.logback.classic.Level.DEBUG
        frameworkLoggers.forEach { name ->
            loggerContext.getLogger(name)?.level = ch.qos.logback.classic.Level.INFO
        }
    } else {
        root.level = ch.qos.logback.classic.Level.WARN
        appLogger.level = ch.qos.logback.classic.Level.WARN
        frameworkLoggers.forEach { name ->
            loggerContext.getLogger(name)?.level = ch.qos.logback.classic.Level.WARN
        }
    }
}
