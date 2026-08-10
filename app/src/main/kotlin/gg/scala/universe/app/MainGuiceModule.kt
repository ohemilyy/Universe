package gg.scala.universe.app

import com.google.inject.AbstractModule
import com.google.inject.Provides
import com.google.inject.TypeLiteral
import gg.scala.universe.command.CommandProvider
import gg.scala.universe.command.CommandProviderImpl
import gg.scala.universe.command.CommandSource
import gg.scala.universe.command.DefaultCommandManager
import gg.scala.universe.cluster.ClusterDataProvider
import gg.scala.universe.cluster.ClusterDataProviderImpl
import gg.scala.universe.config.DatabaseConfigLoader
import gg.scala.universe.config.DatabaseConfiguration
import gg.scala.universe.config.UniverseMainConfiguration
import gg.scala.universe.db.DatabaseProvider
import gg.scala.universe.db.DatabaseRegistry
import gg.scala.universe.db.DatabaseRegistryImpl
import gg.scala.universe.db.H2DatabaseProvider
import gg.scala.universe.db.MySQLDatabaseProvider
import gg.scala.universe.extension.ExtensionService
import gg.scala.universe.metrics.MetricsRegistry
import gg.scala.universe.metrics.MetricsRegistryImpl
import gg.scala.universe.hz.task.TaskDispatcher
import gg.scala.universe.runtime.RuntimeRegistry
import gg.scala.universe.runtime.RuntimeRegistryImpl
import gg.scala.universe.template.DefaultTemplateVariableProvider
import gg.scala.universe.template.TemplateResolver
import gg.scala.universe.template.TemplateStorageRegistry
import gg.scala.universe.template.TemplateStorageRegistryImpl
import gg.scala.universe.template.TemplateSyncService
import gg.scala.universe.template.TemplateVariableRegistry
import gg.scala.universe.template.TemplateVariableRegistryImpl
import gg.scala.universe.service.InstanceCreationService
import gg.scala.universe.service.InstanceSpawner
import gg.scala.universe.service.InstanceStopDispatcher
import org.incendo.cloud.CommandManager
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class MainGuiceModule : AbstractModule() {
    override fun configure() {
        bind(RuntimeRegistry::class.java).to(RuntimeRegistryImpl::class.java).asEagerSingleton()
        bind(TaskDispatcher::class.java).asEagerSingleton()
        bind(InstanceStopDispatcher::class.java).to(TaskDispatcher::class.java)
        bind(InstanceSpawner::class.java).to(InstanceCreationService::class.java)
        bind(CommandProvider::class.java).to(CommandProviderImpl::class.java).asEagerSingleton()
        bind(object : TypeLiteral<CommandManager<CommandSource>>() {}).to(DefaultCommandManager::class.java).asEagerSingleton()
        bind(TemplateStorageRegistry::class.java).to(TemplateStorageRegistryImpl::class.java).asEagerSingleton()
        bind(TemplateVariableRegistry::class.java).to(TemplateVariableRegistryImpl::class.java).asEagerSingleton()
        bind(DefaultTemplateVariableProvider::class.java).asEagerSingleton()
        bind(DatabaseRegistry::class.java).to(DatabaseRegistryImpl::class.java).asEagerSingleton()
        bind(MetricsRegistry::class.java).to(MetricsRegistryImpl::class.java).asEagerSingleton()
        bind(ClusterDataProvider::class.java).to(ClusterDataProviderImpl::class.java).asEagerSingleton()
        bind(TemplateResolver::class.java).to(TemplateSyncService::class.java).asEagerSingleton()
        bind(ExecutorService::class.java).toInstance(Executors.newFixedThreadPool(4))
    }

    @Provides
    fun databaseConfiguration(): DatabaseConfiguration {
        return DatabaseConfigLoader.load()
    }

    @Provides
    fun databaseProvider(
        config: DatabaseConfiguration,
        registry: DatabaseRegistry
    ): DatabaseProvider {
        // Register built-in providers only if selected
        when (config.provider) {
            "h2" -> registry.register("h2", H2DatabaseProvider(config))
            "mysql" -> registry.register("mysql", MySQLDatabaseProvider(config))
        }

        val provider = registry.get(config.provider)
            ?: throw IllegalStateException(
                "Unknown database provider '${config.provider}'. " +
                "Built-in: h2, mysql. Extensions: postgres, mongodb, redis. " +
                "Make sure the corresponding extension JAR is present."
            )

        provider.connect()
        return provider
    }

    @Provides
    fun application(): UniverseApplication {
        return UniverseApplication.instance
    }

    @Provides
    fun mainConfigurationProvider(): UniverseMainConfiguration {
        return UniverseApplication.instance.mainConfiguration
    }

    @Provides
    fun extensionService(): ExtensionService {
        return UniverseApplication.instance.extensionService
    }

}
