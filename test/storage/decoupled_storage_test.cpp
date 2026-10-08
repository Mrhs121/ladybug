#include <filesystem>
#include <fstream>
#include <memory>

#include "common/constants.h"
#include "common/system_config.h"
#include "gtest/gtest.h"
#include "main/connection.h"
#include "main/database.h"
#include "test_helper/test_helper.h"
#include <format>

using namespace lbug::main;
using namespace lbug::common;
using namespace lbug::testing;

class DecoupledStorageTest : public ::testing::Test {
protected:
    void SetUp() override {
        baseDir = TestHelper::getTempDir("decoupled_storage_test");
        if (std::filesystem::exists(baseDir)) {
            std::filesystem::remove_all(baseDir);
        }
        std::filesystem::create_directories(baseDir);
    }

    void TearDown() override {
        if (std::filesystem::exists(baseDir)) {
            std::filesystem::remove_all(baseDir);
        }
    }

    std::string baseDir;
};

TEST_F(DecoupledStorageTest, EndToEndDecoupledStorageTest) {
    auto dbPath = (std::filesystem::path(baseDir) / "decoupled_graph.db").string();

    // 1. Initialize database with enableDecoupledStorage = true
    SystemConfig systemConfig;
    systemConfig.enableDecoupledStorage = true;
    systemConfig.bufferPoolSize = 64 * 1024 * 1024; // 64 MB

    {
        auto db = std::make_unique<Database>(dbPath, systemConfig);
        auto conn = std::make_unique<Connection>(db.get());

        // 2. Create Schema
        auto res = conn->query(
            "CREATE NODE TABLE person (id INT64, name STRING, age INT64, PRIMARY KEY (id));");
        ASSERT_TRUE(res->isSuccess()) << res->toString();

        res = conn->query(
            "CREATE REL TABLE knows (FROM person TO person, weight DOUBLE, MANY_MANY);");
        ASSERT_TRUE(res->isSuccess()) << res->toString();

        // 3. Insert Vertices & Edges
        res = conn->query("CREATE (:person {id: 1, name: 'Alice', age: 30});");
        ASSERT_TRUE(res->isSuccess()) << res->toString();

        res = conn->query("CREATE (:person {id: 2, name: 'Bob', age: 35});");
        ASSERT_TRUE(res->isSuccess()) << res->toString();

        res = conn->query("MATCH (a:person {id: 1}), (b:person {id: 2}) CREATE (a)-[:knows "
                          "{weight: 0.85}]->(b);");
        ASSERT_TRUE(res->isSuccess()) << res->toString();

        // 4. Checkpoint data onto disk
        res = conn->query("CHECKPOINT;");
        ASSERT_TRUE(res->isSuccess()) << res->toString();

        // 5. Query and verify results before closing
        res =
            conn->query("MATCH (a:person)-[r:knows]->(b:person) RETURN a.name, r.weight, b.name;");
        ASSERT_TRUE(res->isSuccess()) << res->toString();
        ASSERT_TRUE(res->hasNext());
        auto tuple = res->getNext();
        EXPECT_EQ(tuple->getValue(0)->getValue<std::string>(), "Alice");
        EXPECT_DOUBLE_EQ(tuple->getValue(1)->getValue<double>(), 0.85);
        EXPECT_EQ(tuple->getValue(2)->getValue<std::string>(), "Bob");
        EXPECT_FALSE(res->hasNext());

        // Close connection and DB
        conn.reset();
        db.reset();
    }

    // 6. Verify Physical File Structure on Disk
    auto dbDir = std::filesystem::path(dbPath);
    EXPECT_TRUE(std::filesystem::is_directory(dbDir));
    EXPECT_TRUE(std::filesystem::exists(dbDir / "metadata.db"));
    EXPECT_TRUE(std::filesystem::exists(dbDir / "topology" / "rel_knows.topo"));
    EXPECT_TRUE(std::filesystem::exists(dbDir / "nodes" / "person" / "props.data"));
    EXPECT_TRUE(std::filesystem::exists(dbDir / "nodes" / "person" / "pk.idx"));
    EXPECT_TRUE(std::filesystem::exists(dbDir / "rels" / "knows" / "props.data"));

    EXPECT_GT(std::filesystem::file_size(dbDir / "metadata.db"), 0u);
    EXPECT_GT(std::filesystem::file_size(dbDir / "topology" / "rel_knows.topo"), 0u);
    EXPECT_GT(std::filesystem::file_size(dbDir / "nodes" / "person" / "props.data"), 0u);
    EXPECT_GT(std::filesystem::file_size(dbDir / "nodes" / "person" / "pk.idx"), 0u);
    EXPECT_GT(std::filesystem::file_size(dbDir / "rels" / "knows" / "props.data"), 0u);

    // 7. Reopen Database (auto-detects decoupled directory DB)
    {
        SystemConfig reopenConfig;
        reopenConfig.bufferPoolSize = 64 * 1024 * 1024;
        auto db = std::make_unique<Database>(dbPath, reopenConfig);
        auto conn = std::make_unique<Connection>(db.get());

        // Verify existing graph data
        auto res =
            conn->query("MATCH (a:person)-[r:knows]->(b:person) RETURN a.name, r.weight, b.name;");
        ASSERT_TRUE(res->isSuccess()) << res->toString();
        ASSERT_TRUE(res->hasNext());
        auto tuple = res->getNext();
        EXPECT_EQ(tuple->getValue(0)->getValue<std::string>(), "Alice");
        EXPECT_DOUBLE_EQ(tuple->getValue(1)->getValue<double>(), 0.85);
        EXPECT_EQ(tuple->getValue(2)->getValue<std::string>(), "Bob");
        EXPECT_FALSE(res->hasNext());

        // Perform additional mutations
        res = conn->query("CREATE (:person {id: 3, name: 'Charlie', age: 25});");
        ASSERT_TRUE(res->isSuccess()) << res->toString();

        res = conn->query("MATCH (p:person) RETURN count(*);");
        ASSERT_TRUE(res->isSuccess()) << res->toString();
        ASSERT_TRUE(res->hasNext());
        EXPECT_EQ(res->getNext()->getValue(0)->getValue<int64_t>(), 3);

        res = conn->query("CHECKPOINT;");
        ASSERT_TRUE(res->isSuccess()) << res->toString();
    }
}

TEST_F(DecoupledStorageTest, BackwardCompatibilitySingleFileTest) {
    auto dbPath = (std::filesystem::path(baseDir) / "single_file.db").string();

    SystemConfig systemConfig;
    systemConfig.enableDecoupledStorage = false;
    systemConfig.bufferPoolSize = 64 * 1024 * 1024;

    {
        auto db = std::make_unique<Database>(dbPath, systemConfig);
        auto conn = std::make_unique<Connection>(db.get());

        auto res =
            conn->query("CREATE NODE TABLE person (id INT64, name STRING, PRIMARY KEY (id));");
        ASSERT_TRUE(res->isSuccess()) << res->toString();

        res = conn->query("CREATE (:person {id: 1, name: 'Alice'});");
        ASSERT_TRUE(res->isSuccess()) << res->toString();

        res = conn->query("CHECKPOINT;");
        ASSERT_TRUE(res->isSuccess()) << res->toString();
    }

    // Verify it is a single file, not a directory
    EXPECT_TRUE(std::filesystem::is_regular_file(dbPath));
    EXPECT_FALSE(std::filesystem::is_directory(dbPath));

    // Reopen single-file database
    {
        SystemConfig reopenConfig;
        reopenConfig.bufferPoolSize = 64 * 1024 * 1024;
        auto db = std::make_unique<Database>(dbPath, reopenConfig);
        auto conn = std::make_unique<Connection>(db.get());

        auto res = conn->query("MATCH (p:person) RETURN p.name;");
        ASSERT_TRUE(res->isSuccess()) << res->toString();
        ASSERT_TRUE(res->hasNext());
        EXPECT_EQ(res->getNext()->getValue(0)->getValue<std::string>(), "Alice");
        EXPECT_FALSE(res->hasNext());
    }
}

class DecoupledStorageCopyTest : public DecoupledStorageTest,
                                 public ::testing::WithParamInterface<bool> {};

TEST_P(DecoupledStorageCopyTest, BulkCopySurvivesRollbackCheckpointAndReopen) {
    const int64_t numRows = StorageConfig::NODE_GROUP_SIZE + 17;
    const auto nodeCSV = (std::filesystem::path(baseDir) / "nodes.csv").string();
    const auto relCSV = (std::filesystem::path(baseDir) / "rels.csv").string();
    {
        std::ofstream nodes{nodeCSV};
        std::ofstream rels{relCSV};
        ASSERT_TRUE(nodes.is_open());
        ASSERT_TRUE(rels.is_open());
        for (auto i = int64_t{0}; i < numRows; i++) {
            nodes << i << ",person_" << i << '\n';
            rels << i << ',' << (i + 1) % numRows << ',' << i << ",edge_" << i << '\n';
        }
        ASSERT_TRUE(nodes.good());
        ASSERT_TRUE(rels.good());
    }

    const auto dbPath = (std::filesystem::path(baseDir) / "bulk_copy.db").string();
    SystemConfig config;
    config.enableDecoupledStorage = GetParam();
    config.autoCheckpoint = false;
    config.forceCheckpointOnClose = false;
    config.maxNumThreads = 2;
    config.bufferPoolSize = 128ULL * 1024 * 1024;

    auto verifyGraph = [&](Connection& conn, int64_t copies) {
        auto res = conn.query("MATCH (n:N) RETURN count(*);");
        ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
        ASSERT_TRUE(res->hasNext());
        EXPECT_EQ(res->getNext()->getValue(0)->getValue<int64_t>(), numRows);

        res = conn.query("MATCH ()-[r:R]->() RETURN count(*), CAST(sum(r.weight) AS INT64);");
        ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
        ASSERT_TRUE(res->hasNext());
        auto row = res->getNext();
        EXPECT_EQ(row->getValue(0)->getValue<int64_t>(), copies * numRows);
        EXPECT_EQ(row->getValue(1)->getValue<int64_t>(), copies * numRows * (numRows - 1) / 2);

        // Read properties from both the full group and the final partial group via the PK index.
        for (auto id : {int64_t{17}, numRows - 1}) {
            res = conn.query(std::format("MATCH (n:N {{id: {}}}) RETURN n.name;", id));
            ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
            ASSERT_TRUE(res->hasNext());
            EXPECT_EQ(res->getNext()->getValue(0)->getValue<std::string>(),
                std::format("person_{}", id));
            EXPECT_FALSE(res->hasNext());
        }

        // Both CSR directions must read the same topology and property values.
        res = conn.query("MATCH (a:N {id: 17})-[r:R]->(b:N) RETURN b.id, r.weight, r.label;");
        ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
        auto seen = int64_t{0};
        while (res->hasNext()) {
            auto row = res->getNext();
            EXPECT_EQ(row->getValue(0)->getValue<int64_t>(), 18);
            EXPECT_EQ(row->getValue(1)->getValue<int64_t>(), 17);
            EXPECT_EQ(row->getValue(2)->getValue<std::string>(), "edge_17");
            seen++;
        }
        EXPECT_EQ(seen, copies);

        res = conn.query("MATCH (a:N)-[r:R]->(b:N {id: 18}) RETURN a.id, r.weight, r.label;");
        ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
        seen = 0;
        while (res->hasNext()) {
            auto row = res->getNext();
            EXPECT_EQ(row->getValue(0)->getValue<int64_t>(), 17);
            EXPECT_EQ(row->getValue(1)->getValue<int64_t>(), 17);
            EXPECT_EQ(row->getValue(2)->getValue<std::string>(), "edge_17");
            seen++;
        }
        EXPECT_EQ(seen, copies);
    };

    {
        Database db{dbPath, config};
        Connection conn{&db};
        for (const auto& query : {"CREATE NODE TABLE N(id INT64 PRIMARY KEY, name STRING);",
                 "CREATE REL TABLE R(FROM N TO N, weight INT64, label STRING);"}) {
            auto res = conn.query(query);
            ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
        }
        const auto copyNodes = std::format("COPY N FROM '{}';", nodeCSV);
        const auto copyRels = std::format("COPY R FROM '{}';", relCSV);

        // Roll back flushed COPY pages, then retry to exercise allocation reuse in each file.
        for (const auto& query :
            {std::string{"BEGIN TRANSACTION;"}, copyNodes, std::string{"ROLLBACK;"}}) {
            auto res = conn.query(query);
            ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
        }
        auto res = conn.query("MATCH (n:N) RETURN count(*);");
        ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
        ASSERT_TRUE(res->hasNext());
        EXPECT_EQ(res->getNext()->getValue(0)->getValue<int64_t>(), 0);

        res = conn.query(copyNodes);
        ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
        for (const auto& query :
            {std::string{"BEGIN TRANSACTION;"}, copyRels, std::string{"ROLLBACK;"}}) {
            auto res = conn.query(query);
            ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
        }
        res = conn.query("MATCH ()-[r:R]->() RETURN count(*);");
        ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
        ASSERT_TRUE(res->hasNext());
        EXPECT_EQ(res->getNext()->getValue(0)->getValue<int64_t>(), 0);

        res = conn.query(copyRels);
        ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
        res = conn.query("CHECKPOINT;");
        ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
        ASSERT_NO_FATAL_FAILURE(verifyGraph(conn, 1));

        // COPY into existing CSR groups, then checkpoint their combined persistent/in-memory data.
        res = conn.query(copyRels);
        ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
        res = conn.query("CHECKPOINT;");
        ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
        ASSERT_NO_FATAL_FAILURE(verifyGraph(conn, 2));
    }

    config.enableDecoupledStorage = false; // Reopening detects a decoupled database directory.
    Database reopened{dbPath, config};
    Connection conn{&reopened};
    ASSERT_NO_FATAL_FAILURE(verifyGraph(conn, 2));
}

INSTANTIATE_TEST_SUITE_P(StorageLayouts, DecoupledStorageCopyTest, ::testing::Values(false, true),
    [](const ::testing::TestParamInfo<bool>& info) {
        return info.param ? "Decoupled" : "SingleFile";
    });

class DecoupledStorageDDLTest : public DecoupledStorageTest,
                                public ::testing::WithParamInterface<bool> {};

TEST_P(DecoupledStorageDDLTest, AddedPropertiesSurviveCheckpointAndReopen) {
    const auto dbPath = (std::filesystem::path(baseDir) / "alter.db").string();
    SystemConfig config;
    config.enableDecoupledStorage = GetParam();
    config.autoCheckpoint = false;
    config.forceCheckpointOnClose = false;
    config.bufferPoolSize = 128ULL * 1024 * 1024;
    auto verify = [](Connection& conn) {
        auto result =
            conn.query("MATCH (n:N) RETURN n.id, n.extra, n.label, n.vals ORDER BY n.id;");
        ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
        for (int64_t id = 0; id < 200; ++id) {
            ASSERT_TRUE(result->hasNext());
            auto row = result->getNext();
            EXPECT_EQ(row->getValue(0)->getValue<int64_t>(), id);
            EXPECT_EQ(row->getValue(1)->getValue<int64_t>(), id * 13);
            EXPECT_EQ(row->getValue(2)->getValue<std::string>(), std::format("node_{}", id));
            EXPECT_EQ(row->getValue(3)->toString(), std::format("[{},{}]", id, id + 1));
        }
        EXPECT_FALSE(result->hasNext());
        // Exercise property reads in both CSR directions.
        for (const auto& query :
            {"MATCH (a:N)-[r:R]->(b:N) RETURN a.id, b.id, r.extra, r.label, r.vals ORDER BY a.id;",
                "MATCH (b:N)<-[r:R]-(a:N) RETURN a.id, b.id, r.extra, r.label, r.vals ORDER BY "
                "a.id;"}) {
            result = conn.query(query);
            ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
            for (int64_t id = 0; id < 200; ++id) {
                ASSERT_TRUE(result->hasNext());
                auto row = result->getNext();
                EXPECT_EQ(row->getValue(0)->getValue<int64_t>(), id);
                EXPECT_EQ(row->getValue(1)->getValue<int64_t>(), (id + 1) % 200);
                EXPECT_EQ(row->getValue(2)->getValue<int64_t>(), id * 17);
                EXPECT_EQ(row->getValue(3)->getValue<std::string>(), std::format("edge_{}", id));
                EXPECT_EQ(row->getValue(4)->toString(), std::format("[edge_{},tag]", id));
            }
            EXPECT_FALSE(result->hasNext());
        }
    };
    {
        Database db{dbPath, config};
        Connection conn{&db};
        for (const auto& query :
            {"CREATE NODE TABLE N(id INT64 PRIMARY KEY);", "CREATE REL TABLE R(FROM N TO N);"}) {
            auto result = conn.query(query);
            ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
        }
        for (int64_t id = 0; id < 200; ++id) {
            ASSERT_TRUE(conn.query(std::format("CREATE (:N {{id: {}}});", id))->isSuccess());
        }
        auto result = conn.query("MATCH (a:N), (b:N) WHERE b.id = (a.id + 1) % 200 "
                                 "CREATE (a)-[:R]->(b);");
        ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
        ASSERT_TRUE(conn.query("CHECKPOINT;")->isSuccess());
        for (const auto& query : {"ALTER TABLE N ADD extra INT64 DEFAULT 42;",
                 "ALTER TABLE N ADD label STRING DEFAULT 'default_node';",
                 "ALTER TABLE N ADD vals INT64[] DEFAULT [1, 2];",
                 "ALTER TABLE R ADD extra INT64 DEFAULT 43;",
                 "ALTER TABLE R ADD label STRING DEFAULT 'default_edge';",
                 "ALTER TABLE R ADD vals STRING[] DEFAULT ['default', 'tag'];"}) {
            result = conn.query(query);
            ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
        }
        result = conn.query("MATCH (a:N {id: 17})-[r:R]->() RETURN a.extra, a.label, "
                            "a.vals, r.extra, r.label, r.vals;");
        ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
        ASSERT_TRUE(result->hasNext());
        auto defaults = result->getNext();
        EXPECT_EQ(defaults->getValue(0)->getValue<int64_t>(), 42);
        EXPECT_EQ(defaults->getValue(1)->getValue<std::string>(), "default_node");
        EXPECT_EQ(defaults->getValue(2)->toString(), "[1,2]");
        EXPECT_EQ(defaults->getValue(3)->getValue<int64_t>(), 43);
        EXPECT_EQ(defaults->getValue(4)->getValue<std::string>(), "default_edge");
        EXPECT_EQ(defaults->getValue(5)->toString(), "[default,tag]");
        EXPECT_FALSE(result->hasNext());
        // Roll back added columns, then checkpoint the committed columns alongside their gaps.
        for (const auto& query :
            {"BEGIN TRANSACTION;", "ALTER TABLE N ADD discarded STRING DEFAULT 'rollback_node';",
                "ALTER TABLE R ADD discarded STRING DEFAULT 'rollback_edge';", "ROLLBACK;"}) {
            result = conn.query(query);
            ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
        }
        EXPECT_FALSE(conn.query("MATCH (n:N) RETURN n.discarded;")->isSuccess());
        EXPECT_FALSE(conn.query("MATCH ()-[r:R]->() RETURN r.discarded;")->isSuccess());
        for (const auto& query :
            {"MATCH (n:N) SET n.extra = n.id * 13, n.label = 'node_' + CAST(n.id AS STRING), "
             "n.vals = [n.id, n.id + 1];",
                "MATCH (a:N)-[r:R]->() SET r.extra = a.id * 17, "
                "r.label = 'edge_' + CAST(a.id AS STRING), "
                "r.vals = ['edge_' + CAST(a.id AS STRING), 'tag'];"}) {
            result = conn.query(query);
            ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
        }
        ASSERT_NO_FATAL_FAILURE(verify(conn));
        result = conn.query("CHECKPOINT;");
        ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
        ASSERT_NO_FATAL_FAILURE(verify(conn));
    }
    config.enableDecoupledStorage = false;
    Database db{dbPath, config};
    Connection conn{&db};
    ASSERT_NO_FATAL_FAILURE(verify(conn));
}

INSTANTIATE_TEST_SUITE_P(StorageLayouts, DecoupledStorageDDLTest, ::testing::Values(false, true),
    [](const ::testing::TestParamInfo<bool>& info) {
        return info.param ? "Decoupled" : "SingleFile";
    });
