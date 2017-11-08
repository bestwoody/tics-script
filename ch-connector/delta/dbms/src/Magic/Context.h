#include "Server/Server.h"

#include <memory>
#include <sys/resource.h>

#include <Poco/Path.h>
#include <Poco/DirectoryIterator.h>

#include <common/ApplicationServerExt.h>
#include <common/ErrorHandlers.h>
#include <common/getMemoryAmount.h>

#include <Common/ClickHouseRevision.h>
#include <Common/CurrentMetrics.h>
#include <Common/Macros.h>
#include <Common/StringUtils.h>
#include <Common/config.h>
#include <Common/getFQDNOrHostName.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/getNumberOfPhysicalCPUCores.h>

#include <Interpreters/AsynchronousMetrics.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/loadMetadata.h>

#include <Storages/MergeTree/ReshardingWorker.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/System/attachSystemTables.h>

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Functions/registerFunctions.h>
#include <TableFunctions/registerTableFunctions.h>

#include "Server/ConfigReloader.h"
#include "Server/MetricsTransmitter.h"
#include "Server/StatusFile.h"

namespace DB
{

std::unique_ptr<Context> createContext(std::string db_path)
{
    auto context = std::make_unique<DB::Context>(DB::Context::createGlobal());
    context->setGlobalContext(*context);
    context->setApplicationType(DB::Context::ApplicationType::SERVER);
    context->setPath(db_path);
    context->setTemporaryPath("/tmp/ch-raw");
    context->setCurrentDatabase("default");
    context->setFlagsPath(db_path + "/flags");
    //context->setUsersConfig("running/config/users.xml");
    return context;
}

static std::string getCanonicalPath(std::string && path)
{
    Poco::trimInPlace(path);
    if (path.empty())
        throw Exception("path configuration parameter is empty");
    if (path.back() != '/')
        path += '/';
    return path;
}

class Application : public Poco::Util::Application
{
public:
    Application(std::string config_path)
    {
        registerFunctions();
        registerAggregateFunctions();
        registerTableFunctions();

        //init();
        loadConfiguration(config_path);
        auto & config = Poco::Util::Application::config();

        global_context = std::make_unique<Context>(Context::createGlobal());
        global_context->setGlobalContext(*global_context);
        global_context->setApplicationType(Context::ApplicationType::SERVER);

        std::string path = getCanonicalPath(config.getString("path"));
        std::string default_database = config.getString("default_database", "default");

        global_context->setPath(path);

        /// Create directories for 'path' and for default database, if not exist.
        Poco::File(path + "data/" + default_database).createDirectories();
        Poco::File(path + "metadata/" + default_database).createDirectories();

        StatusFile status{path + "status"};

        static ServerErrorHandler error_handler;
        Poco::ErrorHandler::set(&error_handler);

        /// Initialize DateLUT early, to not interfere with running time of first query.
        DateLUT::instance();

        /// Directory with temporary data for processing of hard queries.
        {
            std::string tmp_path = config.getString("tmp_path", path + "tmp/");
            global_context->setTemporaryPath(tmp_path);
            Poco::File(tmp_path).createDirectories();

            /// Clearing old temporary files.
            Poco::DirectoryIterator dir_end;
            for (Poco::DirectoryIterator it(tmp_path); it != dir_end; ++it)
            {
                if (it->isFile() && startsWith(it.name(), "tmp"))
                {
                    it->remove();
                }
            }
        }

        Poco::File(path + "flags/").createDirectories();
        global_context->setFlagsPath(path + "flags/");

        if (config.has("macros"))
            global_context->setMacros(Macros(config, "macros"));

        /// Initialize users config reloader.
        std::string users_config_path = config.getString("users_config", config_path);
        /// If path to users' config isn't absolute, try guess its root (current) dir.
        /// At first, try to find it in dir of main config, after will use current dir.
        if (users_config_path.empty() || users_config_path[0] != '/')
        {
            std::string config_dir = Poco::Path(config_path).parent().toString();
            if (Poco::File(config_dir + users_config_path).exists())
                users_config_path = config_dir + users_config_path;
        }

        /// Limit on total number of concurrently executed queries.
        global_context->getProcessList().setMaxSize(config.getInt("max_concurrent_queries", 0));

        /// Setup protection to avoid accidental DROP for big tables (that are greater than 50 GB by default)
        if (config.has("max_table_size_to_drop"))
            global_context->setMaxTableSizeToDrop(config.getUInt64("max_table_size_to_drop"));

        /// Size of cache for uncompressed blocks. Zero means disabled.
        size_t uncompressed_cache_size = config.getUInt64("uncompressed_cache_size", 0);
        if (uncompressed_cache_size)
            global_context->setUncompressedCache(uncompressed_cache_size);

        /// Size of cache for marks (index of MergeTree family of tables). It is necessary.
        size_t mark_cache_size = config.getUInt64("mark_cache_size");
        if (mark_cache_size)
            global_context->setMarkCache(mark_cache_size);

        String default_profile_name = config.getString("default_profile", "default");
        global_context->setDefaultProfileName(default_profile_name);
        //global_context->setSetting("profile", default_profile_name);

        loadMetadataSystem(*global_context);
        /// After the system database is created, attach virtual system tables (in addition to query_log and part_log)
        attachSystemTablesServer(*global_context->getDatabase("system"), false);
        /// Then, load remaining databases
        loadMetadata(*global_context);

        global_context->setCurrentDatabase(default_database);

        {
            /// try to load dictionaries immediately, throw on error and die
            try
            {
                if (!config.getBool("dictionaries_lazy_load", true))
                {
                    global_context->tryCreateEmbeddedDictionaries();
                    global_context->tryCreateExternalDictionaries();
                }
            }
            catch (...)
            {
                throw;
            }

            /// This object will periodically calculate some metrics.
            AsynchronousMetrics async_metrics(*global_context);
            attachSystemTablesAsync(*global_context->getDatabase("system"), async_metrics);

            std::vector<std::unique_ptr<MetricsTransmitter>> metrics_transmitters;
            for (const auto & graphite_key : DB::getMultipleKeysFromConfig(config, "", "graphite"))
            {
                metrics_transmitters.emplace_back(std::make_unique<MetricsTransmitter>(
                    *global_context, async_metrics, graphite_key));
            }

            SessionCleaner session_cleaner(*global_context);
        }
    }

    ~Application()
    {
        global_context->shutdown();
    }

    Context & context()
    {
        return *global_context;
    }

private:
    std::unique_ptr<Context> global_context;
};

}
