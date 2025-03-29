//
//  ThreadingTests.swift
//  SwiftSerializationTests
//
//  Created by Andre Pham on 27/3/2024.
//

import XCTest
@testable import SwiftSerialization

final class ThreadingTests: XCTestCase {

    static let THREAD_COUNT = 5
    static let TIMEOUT = 120
    let databaseTargets: [DatabaseTarget] = [SerializationDatabase()]
    var smallStudent: Student {
        let student = Student(firstName: "Big", lastName: "Boy", debt: 0.0, teacher: self.teacher, subjectNames: ["Math"])
        for _ in 0..<8000 {
            student.giveHomework(Homework(answers: String(Int.random(in: 0..<10_000)), grade: Int.random(in: 0..<10_000)))
        }
        return student
    }
    var mediumStudent: Student {
        let student = Student(firstName: "Big", lastName: "Boy", debt: 0.0, teacher: self.teacher, subjectNames: ["Math"])
        for _ in 0..<40_000 {
            student.giveHomework(Homework(answers: String(Int.random(in: 0..<10_000)), grade: Int.random(in: 0..<10_000)))
        }
        return student
    }
    var largeStudent: Student {
        let student = Student(firstName: "Big", lastName: "Boy", debt: 0.0, teacher: self.teacher, subjectNames: ["Math"])
        for _ in 0..<150_000 {
            student.giveHomework(Homework(answers: String(Int.random(in: 0..<10_000)), grade: Int.random(in: 0..<10_000)))
        }
        return student
    }
    var teacher: Teacher {
        Teacher(firstName: "Karen", lastName: "Kob", salary: 50_000.0)
    }
    
    override func setUp() async throws {
        self.databaseTargets.forEach({ _ = $0.clearDatabase() })
    }
    
    override func tearDown() {
        self.databaseTargets.forEach({ _ = $0.clearDatabase() })
    }

    func testMultipleWriteThreads() throws {
        print("============================== WRITE THREADS ======================")
        // Setup expectations - XCTest doesn't wait for asynchronous code to complete unless explicitly instructed to do so
        let expectedCount = Self.THREAD_COUNT
        let expectation = XCTestExpectation(description: "Complete all threads")
        expectation.expectedFulfillmentCount = expectedCount*self.databaseTargets.count
        // Setup records
        var studentRecords = [Record<Student>]()
        for _ in 0..<expectedCount {
            studentRecords.append(Record(data: self.largeStudent))
        }
        // Test case
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            for (index, record) in studentRecords.enumerated() {
                DispatchQueue.global().async {
                    print("> Writing on thread \(index + 1)")
                    XCTAssert(database.write(record))
                    expectation.fulfill()
                }
            }
        }
        wait(for: [expectation], timeout: TimeInterval(Self.TIMEOUT*self.databaseTargets.count))
        // Make sure all records were written
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            print("> Counting on main thread")
            XCTAssert(database.count() == expectedCount)
        }
        print("============================== END WRITE THREADS ==================")
    }
    
    func testMultipleReadThreads() throws {
        print("============================== READ THREADS =======================")
        // Setup expectations - XCTest doesn't wait for asynchronous code to complete unless explicitly instructed to do so
        let expectedCount = Self.THREAD_COUNT
        let expectation = XCTestExpectation(description: "Complete all threads")
        expectation.expectedFulfillmentCount = expectedCount*self.databaseTargets.count
        // Setup records
        var studentRecords = [Record<Student>]()
        for _ in 0..<expectedCount {
            studentRecords.append(Record(data: self.smallStudent))
        }
        // Test case
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            for (index, record) in studentRecords.enumerated() {
                DispatchQueue.global().async {
                    print("> Writing on thread \(index + 1)")
                    XCTAssert(database.write(record))
                    print("> Reading many on thread \(index + 1)")
                    let read: [Student] = database.read()
                    XCTAssertFalse(read.isEmpty)
                    print("> Reading IDs on thread \(index + 1)")
                    XCTAssertFalse(database.readIDs(Student.self).isEmpty)
                    print("> Reading one on thread \(index + 1)")
                    let student: Student? = database.read(id: record.metadata.id)
                    XCTAssertNotNil(student)
                    expectation.fulfill()
                }
            }
        }
        wait(for: [expectation], timeout: TimeInterval(Self.TIMEOUT*self.databaseTargets.count))
        print("============================== END READ THREADS ===================")
    }
    
    func testMultipleDeleteThreads() throws {
        print("============================== DELETE THREADS =====================")
        // Setup expectations - XCTest doesn't wait for asynchronous code to complete unless explicitly instructed to do so
        let expectedCount = Self.THREAD_COUNT
        let expectation1 = XCTestExpectation(description: "Complete all threads")
        expectation1.expectedFulfillmentCount = expectedCount*self.databaseTargets.count
        let expectation2 = XCTestExpectation(description: "Complete all threads")
        expectation2.expectedFulfillmentCount = expectedCount*self.databaseTargets.count
        // Setup records
        var studentRecords = [Record<Student>]()
        for _ in 0..<expectedCount {
            studentRecords.append(Record(data: self.largeStudent))
        }
        // Test case 1
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            for (index, record) in studentRecords.enumerated() {
                DispatchQueue.global().async {
                    print("> Writing on thread \(index + 1)")
                    XCTAssert(database.write(record))
                    print("> Deleting one on thread \(index + 1)")
                    XCTAssert(database.delete(id: record.metadata.id))
                    expectation1.fulfill()
                }
            }
        }
        wait(for: [expectation1], timeout: TimeInterval(Self.TIMEOUT*self.databaseTargets.count))
        // Test case 2
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            for index in 0..<Self.THREAD_COUNT {
                DispatchQueue.global().async {
                    print("> Deleting many on thread \(index + 1)")
                    let _ = database.delete(Student.self)
                    print("> Deleting all on thread \(index + 1)")
                    let _ = database.clearDatabase()
                    expectation2.fulfill()
                }
            }
        }
        wait(for: [expectation2], timeout: TimeInterval(Self.TIMEOUT*self.databaseTargets.count))
        print("============================== END DELETE THREADS =================")
    }
    
    func testMultipleCountThreads() throws {
        print("============================== COUNT THREADS ======================")
        // Setup expectations - XCTest doesn't wait for asynchronous code to complete unless explicitly instructed to do so
        let expectedCount = Self.THREAD_COUNT
        let expectation = XCTestExpectation(description: "Complete all threads")
        expectation.expectedFulfillmentCount = expectedCount*self.databaseTargets.count
        // Setup records
        var studentRecords = [Record<Student>]()
        for _ in 0..<expectedCount {
            studentRecords.append(Record(data: self.largeStudent))
        }
        // Test case
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            for (index, record) in studentRecords.enumerated() {
                DispatchQueue.global().async {
                    print("> Writing on thread \(index + 1)")
                    XCTAssert(database.write(record))
                    print("> Counting all on thread \(index + 1)")
                    XCTAssert(database.count() > 0)
                    print("> Counting on thread \(index + 1)")
                    XCTAssert(database.count(Student.self) > 0)
                    expectation.fulfill()
                }
            }
        }
        wait(for: [expectation], timeout: TimeInterval(Self.TIMEOUT*self.databaseTargets.count))
        print("============================== END COUNT THREADS ==================")
    }
    
    func testMultipleTransactionThreads() throws {
        print("============================== TRANSACTION THREADS ================")
        // Setup expectations - XCTest doesn't wait for asynchronous code to complete unless explicitly instructed to do so
        let expectedCount = Self.THREAD_COUNT
        let expectation = XCTestExpectation(description: "Complete all threads")
        expectation.expectedFulfillmentCount = expectedCount*self.databaseTargets.count
        // Setup records
        var studentRecords = [Record<Student>]()
        for _ in 0..<expectedCount {
            studentRecords.append(Record(data: self.largeStudent))
        }
        // Test case
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            for (index, record) in studentRecords.enumerated() {
                DispatchQueue.global().async {
                    // Depending on threads, there isn't always a transaction to override (and hence rollback) / commit / rollback
                    // We execute a these operations to ensure thread access is safe and valid (otherwise an error is thrown)
                    // As to the actual order - this wouldn't be proper code in an application
                    // Applications are expected to manage transaction operation order - you shouldn't be starting multiple concurrent transactions simultaneously
                    // (That defeats the purpose of being able to access the database from anywhere, and not having to complete a transaction within a block)
                    print("> Starting transaction on thread \(index + 1)")
                    let _ = database.startTransaction(override: true)
                    print("> Writing on thread \(index + 1)")
                    XCTAssert(database.write(record))
                    print("> Completing transaction on thread \(index + 1)")
                    let _ = database.commitTransaction()
                    print("> Starting transaction on thread \(index + 1)")
                    let _ = database.startTransaction(override: true)
                    print("> Writing on thread \(index + 1)")
                    XCTAssert(database.write(record))
                    print("> Rolling back transaction on thread \(index + 1)")
                    let _ = database.rollbackTransaction()
                    expectation.fulfill()
                }
            }
        }
        wait(for: [expectation], timeout: TimeInterval(Self.TIMEOUT*self.databaseTargets.count))
        print("============================== END TRANSACTION THREADS ============")
    }

}
