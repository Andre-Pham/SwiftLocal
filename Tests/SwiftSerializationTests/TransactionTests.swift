//
//  TransactionTests.swift
//  SwiftSerialization
//
//  Created by Andre Pham on 8/11/2023.
//

import XCTest
@testable import SwiftSerialization

final class TransactionTests: XCTestCase {

    let databaseTargets: [DatabaseTarget] = [SerializationDatabase()]
    var student1: Student {
        Student(firstName: "Billy", lastName: "Bob", debt: 100_000.0, teacher: self.teacher, subjectNames: ["Physics", "English"])
    }
    var student2: Student {
        Student(firstName: "Sammy", lastName: "Sob", debt: 0.0, teacher: self.teacher, subjectNames: ["Math"])
    }
    var teacher: Teacher {
        Teacher(firstName: "Karen", lastName: "Kob", salary: 50_000.0)
    }
    
    override func setUp() async throws {
        databaseTargets.forEach({ _ = $0.clearDatabase() })
    }
    
    override func tearDown() {
        databaseTargets.forEach({ _ = $0.clearDatabase() })
    }
    
    func testCommitTransaction() throws {
        for database in databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            let record = Record(data: self.student1)
            XCTAssert(database.count() == 0)
            XCTAssert(database.startTransaction(override: true))
            XCTAssert(database.write(record))
            // If we write during a transaction we expect the changes to have been applied
            XCTAssert(database.count() == 1)
            XCTAssert(database.commitTransaction())
            // After committing we expect the changes to have been applied
            XCTAssert(database.count() == 1)
            // If we've committed we expect a rollback to fail (return false)
            XCTAssertFalse(database.rollbackTransaction())
            // After rolling back a non-existent transaction we expect our record to still be there
            XCTAssert(database.count() == 1)
        }
    }
    
    func testRollbackTransaction() throws {
        for database in databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            let record = Record(data: self.student1)
            XCTAssert(database.count() == 0)
            XCTAssert(database.startTransaction(override: true))
            XCTAssert(database.write(record))
            XCTAssert(database.count() == 1)
            XCTAssert(database.rollbackTransaction())
            // After rolling back we expect our record that was previously there to have been removed
            XCTAssert(database.count() == 0)
        }
    }
    
    func testCommitThenRollback() throws {
        for database in databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            let record0 = Record(data: self.teacher)
            let record1 = Record(data: self.student1)
            let record2 = Record(data: self.student2)
            // First we write one record (no transaction necessary)
            XCTAssert(database.write(record0))
            XCTAssert(database.count() == 1)
            // Then we write one record using a transaction, and commit
            XCTAssert(database.startTransaction(override: true))
            XCTAssert(database.write(record1))
            XCTAssert(database.commitTransaction())
            XCTAssert(database.count() == 2)
            // Then we write one record using a transaction, then rollback
            XCTAssert(database.startTransaction(override: true))
            XCTAssert(database.write(record2))
            XCTAssert(database.count() == 3)
            XCTAssert(database.rollbackTransaction())
            // After rolling back, we expect our previous state of two records
            XCTAssert(database.count() == 2)
        }
    }
    
    func testTransactionOverride() throws {
        for database in databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            let record = Record(data: self.student1)
            XCTAssert(database.startTransaction(override: true))
            XCTAssert(database.write(record))
            XCTAssert(database.count() == 1)
            XCTAssert(database.startTransaction(override: true))
            // After we start a transaction during another transaction with override true
            // we expect the previous transaction's writes to be undone
            XCTAssert(database.count() == 0)
            XCTAssert(database.commitTransaction())
        }
    }
    
    func testTransactionNoOverride() throws {
        for database in databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            let record = Record(data: self.student1)
            XCTAssert(database.startTransaction(override: true))
            XCTAssert(database.write(record))
            XCTAssert(database.count() == 1)
            XCTAssertFalse(database.startTransaction(override: false))
            // After we start a transaction during another transaction with override false
            // we expect the previous transaction's writes to persist
            XCTAssert(database.count() == 1)
            XCTAssert(database.rollbackTransaction())
            // But after rolling back, we still expect the previous transaction's writes to be undone
            XCTAssert(database.count() == 0)
        }
    }
    
    func testTransactionManyCommit() throws {
        for database in databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            let record = Record(data: self.student1)
            XCTAssert(database.startTransaction(override: true))
            XCTAssert(database.write(record))
            XCTAssert(database.count() == 1)
            XCTAssert(database.commitTransaction())
            // If we commit a second time, we expect it to fail (return false) and the previous state to persist
            XCTAssertFalse(database.commitTransaction())
            XCTAssert(database.count() == 1)
        }
    }
    
    func testTransactionManyRollback() throws {
        for database in databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            let record = Record(data: self.student1)
            XCTAssert(database.startTransaction(override: true))
            XCTAssert(database.write(record))
            XCTAssert(database.count() == 1)
            XCTAssert(database.rollbackTransaction())
            XCTAssert(database.count() == 0)
            // If we rollback a second time, we expect it to fail (return false) and the previous state to persist
            XCTAssertFalse(database.rollbackTransaction())
            XCTAssert(database.count() == 0)
        }
    }

}
