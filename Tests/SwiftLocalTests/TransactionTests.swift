//
//  TransactionTests.swift
//  SwiftLocal
//
//  Created by Andre Pham on 8/11/2023.
//

import XCTest
@testable import SwiftLocal

final class TransactionTests: XCTestCase {

    let localDatabase = LocalDatabase()
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
        self.localDatabase.clearDatabase()
    }
    
    override func tearDown() {
        self.localDatabase.clearDatabase()
    }
    
    func testCommitTransaction() throws {
        let record = Record(data: self.student1)
        XCTAssert(self.localDatabase.count() == 0)
        XCTAssert(self.localDatabase.startTransaction(override: true))
        XCTAssert(self.localDatabase.write(record))
        // If we write during a transaction we expect the changes to have been applied
        XCTAssert(self.localDatabase.count() == 1)
        XCTAssert(self.localDatabase.commitTransaction())
        // After committing we expect the changes to have been applied
        XCTAssert(self.localDatabase.count() == 1)
        // If we've committed we expect a rollback to fail (return false)
        XCTAssertFalse(self.localDatabase.rollbackTransaction())
        // After rolling back a non-existent transaction we expect our record to still be there
        XCTAssert(self.localDatabase.count() == 1)
    }
    
    func testRollbackTransaction() throws {
        let record = Record(data: self.student1)
        XCTAssert(self.localDatabase.count() == 0)
        XCTAssert(self.localDatabase.startTransaction(override: true))
        XCTAssert(self.localDatabase.write(record))
        XCTAssert(self.localDatabase.count() == 1)
        XCTAssert(self.localDatabase.rollbackTransaction())
        // After rolling back we expect our record that was previously there to have been removed
        XCTAssert(self.localDatabase.count() == 0)
    }
    
    func testCommitThenRollback() throws {
        let record0 = Record(data: self.teacher)
        let record1 = Record(data: self.student1)
        let record2 = Record(data: self.student2)
        // First we write one record (no transaction necessary)
        XCTAssert(self.localDatabase.write(record0))
        XCTAssert(self.localDatabase.count() == 1)
        // Then we write one record using a transaction, and commit
        XCTAssert(self.localDatabase.startTransaction(override: true))
        XCTAssert(self.localDatabase.write(record1))
        XCTAssert(self.localDatabase.commitTransaction())
        XCTAssert(self.localDatabase.count() == 2)
        // Then we write one record using a transaction, then rollback
        XCTAssert(self.localDatabase.startTransaction(override: true))
        XCTAssert(self.localDatabase.write(record2))
        XCTAssert(self.localDatabase.count() == 3)
        XCTAssert(self.localDatabase.rollbackTransaction())
        // After rolling back, we expect our previous state of two records
        XCTAssert(self.localDatabase.count() == 2)
    }
    
    func testTransactionOverride() throws {
        let record = Record(data: self.student1)
        XCTAssert(self.localDatabase.startTransaction(override: true))
        XCTAssert(self.localDatabase.write(record))
        XCTAssert(self.localDatabase.count() == 1)
        XCTAssert(self.localDatabase.startTransaction(override: true))
        // After we start a transaction during another transaction with override true
        // we expect the previous transaction's writes to be undone
        XCTAssert(self.localDatabase.count() == 0)
        XCTAssert(self.localDatabase.commitTransaction())
    }
    
    func testTransactionNoOverride() throws {
        let record = Record(data: self.student1)
        XCTAssert(self.localDatabase.startTransaction(override: true))
        XCTAssert(self.localDatabase.write(record))
        XCTAssert(self.localDatabase.count() == 1)
        XCTAssertFalse(self.localDatabase.startTransaction(override: false))
        // After we start a transaction during another transaction with override false
        // we expect the previous transaction's writes to persist
        XCTAssert(self.localDatabase.count() == 1)
        XCTAssert(self.localDatabase.rollbackTransaction())
        // But after rolling back, we still expect the previous transaction's writes to be undone
        XCTAssert(self.localDatabase.count() == 0)
    }
    
    func testTransactionManyCommit() throws {
        let record = Record(data: self.student1)
        XCTAssert(self.localDatabase.startTransaction(override: true))
        XCTAssert(self.localDatabase.write(record))
        XCTAssert(self.localDatabase.count() == 1)
        XCTAssert(self.localDatabase.commitTransaction())
        // If we commit a second time, we expect it to fail (return false) and the previous state to persist
        XCTAssertFalse(self.localDatabase.commitTransaction())
        XCTAssert(self.localDatabase.count() == 1)
    }
    
    func testTransactionManyRollback() throws {
        let record = Record(data: self.student1)
        XCTAssert(self.localDatabase.startTransaction(override: true))
        XCTAssert(self.localDatabase.write(record))
        XCTAssert(self.localDatabase.count() == 1)
        XCTAssert(self.localDatabase.rollbackTransaction())
        XCTAssert(self.localDatabase.count() == 0)
        // If we rollback a second time, we expect it to fail (return false) and the previous state to persist
        XCTAssertFalse(self.localDatabase.rollbackTransaction())
        XCTAssert(self.localDatabase.count() == 0)
    }

}
