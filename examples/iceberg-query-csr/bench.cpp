// Time both engines up to a fully materialized result, without client row conversion or printing.
#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

#include "duckdb.hpp"
#include "main/connection.h"
#include "main/database.h"
#include "processor/result/flat_tuple.h"

using Clock = std::chrono::steady_clock;

static std::string readFile(const std::string& path) {
    std::ifstream in(path);
    if (!in) {
        throw std::runtime_error("Cannot read " + path);
    }
    return {std::istreambuf_iterator<char>(in), {}};
}

static std::string cypherString(const std::string& value) {
    std::string literal = "'";
    for (auto c : value) {
        if (c == '\'' || c == '\\') {
            literal += '\\';
        }
        literal += c;
    }
    return literal + "'";
}

static void run(char** args) {
    const auto repeats = std::stoi(args[5]);
    auto cypher = readFile(args[3]), sql = readFile(args[4]);
    const bool sidecar = args[6] != nullptr;
    lbug::main::Database db(sidecar ? args[6] : ":memory:",
        lbug::main::SystemConfig(512ULL << 20, 1));
    lbug::main::Connection conn(&db);
    auto graph = [&](const std::string& q) {
        auto result = conn.query(q);
        if (!result->isSuccess()) {
            throw std::runtime_error(result->getErrorMessage());
        }
        return result;
    };
    duckdb::DuckDB sqlDb(nullptr);
    duckdb::Connection sqlConn(sqlDb);
    auto relational = [&](const std::string& q) {
        auto result = sqlConn.Query(q);
        if (result->HasError()) {
            throw std::runtime_error(result->GetError());
        }
        return result;
    };
    graph("CALL iceberg_poc_catalog=" + cypherString(args[2]));
    std::istringstream ddl(readFile(args[1]));
    for (std::string statement; std::getline(ddl, statement, ';');) {
        if (statement.find_first_not_of(" \r\n\t") != std::string::npos) {
            graph(statement);
        }
    }
    relational("SET threads=1");
    relational("LOAD iceberg");

    uint64_t rows;
    {
        // Warm the files and check every cell once on the identical, ordered result.
        auto start = Clock::now();
        auto actual = graph(cypher);
        if (sidecar) {
            auto ms = std::chrono::duration<double, std::milli>(Clock::now() - start).count();
            std::cout << "first_build,0," << ms << ',' << actual->getNumTuples() << '\n';
        }
        auto reference = relational(sql);
        rows = actual->getNumTuples();
        if (rows != reference->RowCount() || actual->getNumColumns() != reference->ColumnCount()) {
            throw std::runtime_error("MATCH/JOIN shape differs");
        }
        auto referenceRows = reference->Collection().GetRows();
        for (uint64_t r = 0; r < rows; ++r) {
            auto tuple = actual->getNext();
            for (size_t c = 0; c < actual->getNumColumns(); ++c) {
                if (tuple->getValue(c)->toString() != referenceRows.GetValue(c, r).ToString()) {
                    throw std::runtime_error(
                        "MATCH/JOIN values differ at row " + std::to_string(r));
                }
            }
        }
    }
    auto measure = [&](const char* mode, int rep, const auto& query, const auto& count) {
        auto start = Clock::now();
        auto result = query();
        auto ms = std::chrono::duration<double, std::milli>(Clock::now() - start).count();
        if (count(*result) != rows) {
            throw std::runtime_error("Result size changed");
        }
        std::cout << mode << ',' << rep << ',' << ms << ',' << rows << '\n';
    };
    auto qGraph = [&] { return graph(cypher); };
    auto qJoin = [&] { return relational(sql); };
    auto graphRows = [](auto& r) { return r.getNumTuples(); };
    auto sqlRows = [](auto& r) { return r.RowCount(); };
    for (int i = 0; i < repeats; ++i) {
        if (i % 2 == 0) {
            measure("join", i, qJoin, sqlRows);
        }
        measure(sidecar ? "sidecar_hit" : "rebuild", i, qGraph, graphRows);
        if (i % 2 == 1) {
            measure("join", i, qJoin, sqlRows);
        }
    }
    graph("BEGIN TRANSACTION READ ONLY");
    graph(cypher); // Populate CSR and all projected columns in this transaction.
    for (int i = 0; i < repeats; ++i) {
        measure("reuse", i, qGraph, graphRows);
    }
    graph("COMMIT");
}

int main(int argc, char** argv) {
    if (argc != 6 && argc != 7) {
        std::cerr << "Usage: bench SCHEMA CATALOG CYPHER SQL REPEATS [NEW_GRAPH_DB]\n";
        return 2;
    }
    try {
        run(argv);
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << '\n';
        return 1;
    }
}
