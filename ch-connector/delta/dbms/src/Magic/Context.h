#pragma once

#include "Server/Server.h"

#include <memory>
#include <sys/resource.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/Net/NetException.h>

#include <ext/scope_guard.h>

#include <common/ApplicationServerExt.h>
#include <common/ErrorHandlers.h>
#include <common/getMemoryAmount.h>

#include <Common/ClickHouseRevision.h>
#include <Common/CurrentMetrics.h>
#include <Common/Macros.h>
#include <Common/StringUtils.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperNodeCache.h>
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
#include "Server/TCPHandlerFactory.h"

#if Poco_NetSSL_FOUND
#include <Poco/Net/Context.h>
#include <Poco/Net/SecureServerSocket.h>
#endif

#include <Common/typeid_cast.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>

#include "BlockOutputStreamPrintRows.h"

namespace DB
{

class Application : public BaseDaemon
{
public:
    // TODO: manually create config and context
    Application(int argc, char ** argv) : BaseDaemon() {
        BaseDaemon::run(argc, argv);
    }

    int main(const std::vector<std::string> & args) override
    {
        Logger * log = &logger();
        LOG_DEBUG(log, "Creating application.");

        registerFunctions();
        registerAggregateFunctions();
        registerTableFunctions();

        /// CurrentMetrics::set(CurrentMetrics::Revision, ClickHouseRevision::get());

        std::string config_path = config().getString("config-file", "config.xml");

        /** Context contains all that query execution is dependent:
          *  settings, available functions, data types, aggregate functions, databases...
          */
        global_context = std::make_unique<Context>(Context::createGlobal());
        global_context->setGlobalContext(*global_context);
        global_context->setApplicationType(Context::ApplicationType::SERVER);

        bool has_zookeeper = false;
        zkutil::ZooKeeperNodeCache main_config_zk_node_cache([&] { return global_context->getZooKeeper(); });

        std::string path = getCanonicalPath(config().getString("path"));
        std::string default_database = config().getString("default_database", "default");

        global_context->setPath(path);

        /// Create directories for 'path' and for default database, if not exist.
        Poco::File(path + "data/" + default_database).createDirectories();
        Poco::File(path + "metadata/" + default_database).createDirectories();

        StatusFile status{path + "status"};

        static ServerErrorHandler error_handler;
        Poco::ErrorHandler::set(&error_handler);

        /// Initialize DateLUT early, to not interfere with running time of first query.
        LOG_DEBUG(log, "Initializing DateLUT.");
        DateLUT::instance();
        LOG_TRACE(log, "Initialized DateLUT with time zone `" << DateLUT::instance().getTimeZone() << "'.");

        /// Directory with temporary data for processing of hard queries.
        {
            std::string tmp_path = config().getString("tmp_path", path + "tmp/");
            global_context->setTemporaryPath(tmp_path);
            Poco::File(tmp_path).createDirectories();

            /// Clearing old temporary files.
            Poco::DirectoryIterator dir_end;
            for (Poco::DirectoryIterator it(tmp_path); it != dir_end; ++it)
            {
                if (it->isFile() && startsWith(it.name(), "tmp"))
                {
                    LOG_DEBUG(log, "Removing old temporary file " << it->path());
                    it->remove();
                }
            }
        }

        /** Directory with 'flags': files indicating temporary settings for the server set by system administrator.
          * Flags may be cleared automatically after being applied by the server.
          * Examples: do repair of local data; clone all replicated tables from replica.
          */
        Poco::File(path + "flags/").createDirectories();
        global_context->setFlagsPath(path + "flags/");

        /// Initialize main config reloader.
        std::string include_from_path = config().getString("include_from", "/etc/metrika.xml");
        auto main_config_reloader = std::make_unique<ConfigReloader>(config_path,
            include_from_path,
            std::move(main_config_zk_node_cache),
            [&](ConfigurationPtr config) { global_context->setClustersConfig(config); },
            /* already_loaded = */ true);

        /// Initialize users config reloader.
        std::string users_config_path = config().getString("users_config", config_path);
        /// If path to users' config isn't absolute, try guess its root (current) dir.
        /// At first, try to find it in dir of main config, after will use current dir.
        if (users_config_path.empty() || users_config_path[0] != '/')
        {
            std::string config_dir = Poco::Path(config_path).parent().toString();
            if (Poco::File(config_dir + users_config_path).exists())
                users_config_path = config_dir + users_config_path;
        }
        auto users_config_reloader = std::make_unique<ConfigReloader>(users_config_path,
            include_from_path,
            zkutil::ZooKeeperNodeCache([&] { return global_context->getZooKeeper(); }),
            [&](ConfigurationPtr config) { global_context->setUsersConfig(config); },
            /* already_loaded = */ false);

        /// Limit on total number of concurrently executed queries.
        global_context->getProcessList().setMaxSize(config().getInt("max_concurrent_queries", 0));

        /// Setup protection to avoid accidental DROP for big tables (that are greater than 50 GB by default)
        if (config().has("max_table_size_to_drop"))
            global_context->setMaxTableSizeToDrop(config().getUInt64("max_table_size_to_drop"));

        /// Size of cache for uncompressed blocks. Zero means disabled.
        size_t uncompressed_cache_size = config().getUInt64("uncompressed_cache_size", 0);
        if (uncompressed_cache_size)
            global_context->setUncompressedCache(uncompressed_cache_size);

        /// Size of cache for marks (index of MergeTree family of tables). It is necessary.
        size_t mark_cache_size = config().getUInt64("mark_cache_size");
        if (mark_cache_size)
            global_context->setMarkCache(mark_cache_size);

        String default_profile_name = config().getString("default_profile", "default");
        global_context->setDefaultProfileName(default_profile_name);
        global_context->setSetting("profile", default_profile_name);

        LOG_INFO(log, "Loading metadata.");
        loadMetadataSystem(*global_context);
        /// After the system database is created, attach virtual system tables (in addition to query_log and part_log)
        attachSystemTablesServer(*global_context->getDatabase("system"), has_zookeeper);
        /// Then, load remaining databases
        loadMetadata(*global_context);
        LOG_DEBUG(log, "Loaded metadata.");

        global_context->setCurrentDatabase(default_database);

        return 0;
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
    std::string getCanonicalPath(const std::string & origin)
    {
        std::string path = origin;
        Poco::trimInPlace(path);
        if (path.empty())
            throw Exception("path configuration parameter is empty");
        if (path.back() != '/')
            path += '/';
        return path;
    }

private:
    std::unique_ptr<Context> global_context;
};

}
