//
//  TransactionTests.swift
//  SwiftLocal
//
//  Created by Andre Pham on 8/11/2023.
//

import XCTest
@testable import SwiftLocal

final class TransactionTests: XCTestCase {

    let localDatabase = try! LocalDatabase()
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
        print("TransactionTests - setUp")
        try await self.localDatabase.clearDatabase()
    }
    
    override func tearDown() async throws {
        print("TransactionTests - tearDown")
        try await self.localDatabase.clearDatabase()
    }
    
    func testCommitTransaction() async throws {
        let record = Record(data: self.student1)
        var count = try await self.localDatabase.count()
        XCTAssertEqual(count, 0)
        // Start transaction
        try await self.localDatabase.startTransaction(override: true)
        try await self.localDatabase.write(record)
        // If we write during a transaction we expect the changes to have been applied
        count = try await self.localDatabase.count()
        XCTAssertEqual(count, 1)
        // Commit the transaction
        try await self.localDatabase.commitTransaction()
        // After committing we expect the changes to have been applied
        count = try await self.localDatabase.count()
        XCTAssertEqual(count, 1)
        // If we've committed, there is no active transaction, so attempting a rollback should throw
        do {
            try await self.localDatabase.rollbackTransaction()
            XCTFail("Expected rollbackTransaction() to throw when no active transaction exists")
        } catch {
            // Happy path - catch expected error
        }
        // After trying to rollback a non-existent transaction, we expect our record to still be there
        count = try await self.localDatabase.count()
        XCTAssertEqual(count, 1)
    }
    
    func testRollbackTransaction() async throws {
        let record = Record(data: self.student1)
        var count = try await self.localDatabase.count()
        XCTAssertEqual(count, 0)
        // Start transaction
        try await self.localDatabase.startTransaction(override: true)
        try await self.localDatabase.write(record)
        count = try await self.localDatabase.count()
        XCTAssertEqual(count, 1)
        // Rollback transaction
        try await self.localDatabase.rollbackTransaction()
        // After rolling back we expect our record that was previously there to have been removed
        count = try await self.localDatabase.count()
        XCTAssertEqual(count, 0)
    }
    
    func testCommitThenRollback() async throws {
        let record0 = Record(data: self.teacher)
        let record1 = Record(data: self.student1)
        let record2 = Record(data: self.student2)
        // First we write one record (no transaction necessary)
        try await self.localDatabase.write(record0)
        var count = try await self.localDatabase.count()
        XCTAssertEqual(count, 1)
        // Then we write one record using a transaction, and commit
        try await self.localDatabase.startTransaction(override: true)
        try await self.localDatabase.write(record1)
        try await self.localDatabase.commitTransaction()
        count = try await self.localDatabase.count()
        XCTAssertEqual(count, 2)
        // Then we write one record using a transaction, then rollback
        try await self.localDatabase.startTransaction(override: true)
        try await self.localDatabase.write(record2)
        count = try await self.localDatabase.count()
        XCTAssertEqual(count, 3)
        try await self.localDatabase.rollbackTransaction()
        // After rolling back, we expect our previous state of two records
        count = try await self.localDatabase.count()
        XCTAssertEqual(count, 2)
    }
    
    func testTransactionOverride() async throws {
        let record = Record(data: self.student1)
        try await self.localDatabase.startTransaction(override: true)
        try await self.localDatabase.write(record)
        var count = try await self.localDatabase.count()
        XCTAssertEqual(count, 1)
        // After we start a transaction during another transaction with override true
        // we expect the previous transaction's writes to be undone
        try await self.localDatabase.startTransaction(override: true)
        count = try await self.localDatabase.count()
        XCTAssertEqual(count, 0)
        try await self.localDatabase.commitTransaction()
        count = try await self.localDatabase.count()
        XCTAssertEqual(count, 0)
    }
    
    func testTransactionNoOverride() async throws {
        let record = Record(data: self.student1)
        try await self.localDatabase.startTransaction(override: true)
        try await self.localDatabase.write(record)
        var count = try await self.localDatabase.count()
        XCTAssertEqual(count, 1)
        // After we attempt to start a transaction during another transaction with override false
        // we expect an error to be thrown
        do {
            try await self.localDatabase.startTransaction(override: false)
            XCTFail("Expected startTransaction(override: false) to throw when a transaction is already active")
        } catch {
            // Happy path - catch expected error
        }
        // We expect the initial transaction's writes to persist
        count = try await self.localDatabase.count()
        XCTAssertEqual(count, 1)
        // After rolling back, we expect the previous transaction's writes to be undone
        try await self.localDatabase.rollbackTransaction()
        count = try await self.localDatabase.count()
        XCTAssertEqual(count, 0)
    }
    
    func testTransactionManyCommit() async throws {
        let record = Record(data: self.student1)
        try await self.localDatabase.startTransaction(override: true)
        try await self.localDatabase.write(record)
        var count = try await self.localDatabase.count()
        XCTAssertEqual(count, 1)
        try await self.localDatabase.commitTransaction()
        // If we commit a second time, we expect it to throw because there is no active transaction
        do {
            try await self.localDatabase.commitTransaction()
            XCTFail("Expected commitTransaction() to throw when no active transaction exists")
        } catch {
            // Happy path - catch expected error
        }
        // We expect the initial transaction's writes to persist
        count = try await self.localDatabase.count()
        XCTAssertEqual(count, 1)
    }
    
    func testTransactionManyRollback() async throws {
        let record = Record(data: self.student1)
        try await self.localDatabase.startTransaction(override: true)
        try await self.localDatabase.write(record)
        var count = try await self.localDatabase.count()
        XCTAssertEqual(count, 1)
        try await self.localDatabase.rollbackTransaction()
        count = try await self.localDatabase.count()
        XCTAssertEqual(count, 0)
        // If we rollback a second time, we expect it to throw because there is no active transaction
        do {
            try await self.localDatabase.rollbackTransaction()
            XCTFail("Expected rollbackTransaction() to throw when no active transaction exists")
        } catch {
            // Happy path - catch expected error
        }
        // We expect the initial state to persist
        count = try await self.localDatabase.count()
        XCTAssertEqual(count, 0)
    }

}
