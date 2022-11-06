//
//  QuackTests.swift
//  QuackTests
//
//  Created by Mathijs Kadijk on 06/11/2022.
//

import XCTest
import duckdb

final class QuackTests: XCTestCase {

    override func setUpWithError() throws {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDownWithError() throws {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    func testExample() throws {
        var db: duckdb_database? = nil
        if (duckdb_open(":memory:", &db) == DuckDBError) {
            return XCTFail("Open failure")
        }
        
        var con: duckdb_connection? = nil
        if (duckdb_connect(db, &con) == DuckDBError) {
            return XCTFail("Connect failure")
        }
        
        var state: duckdb_state;
        var result: duckdb_result = duckdb_result();

        // create a table
        state = duckdb_query(con, "CREATE TABLE integers(i INTEGER, j INTEGER);", nil);
        if (state == DuckDBError) {
            return XCTFail("CREATE TABLE failure")
        }
        // insert three rows into the table
        state = duckdb_query(con, "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);", nil);
        if (state == DuckDBError) {
            return XCTFail("INSERT INTO failure")
        }
        // query rows again
        state = duckdb_query(con, "SELECT * FROM integers", &result);
        if (state == DuckDBError) {
            return XCTFail("SELECT failure")
        }
        
        // print the above result to CSV format using `duckdb_value_varchar`
        let row_count: idx_t = duckdb_row_count(&result);
        let column_count: idx_t = duckdb_column_count(&result);
        for row in 0..<row_count {
            for col in 0..<column_count {
                if (col > 0) { print(",", terminator: "") }
                if let str_val = duckdb_value_varchar(&result, col, row) {
                    print(String(cString: str_val), terminator: "");
                    duckdb_free(str_val)
                } else {
                    print("NULL", terminator: "");
                }
           }
           print("")
        }

        // destroy the result after we are done with it
        duckdb_destroy_result(&result);
        
        duckdb_disconnect(&con);
        duckdb_close(&db);
    }

}
