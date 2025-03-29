//
//  ThreadingTests.swift
//  SwiftLocalTests
//
//  Created by Andre Pham on 27/3/2024.
//

import XCTest
@testable import SwiftLocal

final class ThreadingTests: XCTestCase {

    static let THREAD_COUNT = 5
    static let TIMEOUT = 120
    let localDatabase = LocalDatabase()
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
        self.localDatabase.clearDatabase()
    }
    
    override func tearDown() {
        self.localDatabase.clearDatabase()
    }

    func testMultipleWriteThreads() throws {
        print("============================== WRITE THREADS ======================")
        // Setup expectations - XCTest doesn't wait for asynchronous code to complete unless explicitly instructed to do so
        let expectedCount = Self.THREAD_COUNT
        let expectation = XCTestExpectation(description: "Complete all threads")
        expectation.expectedFulfillmentCount = expectedCount
        // Setup records
        var studentRecords = [Record<Student>]()
        for _ in 0..<expectedCount {
            studentRecords.append(Record(data: self.largeStudent))
        }
        // Test case
        for (index, record) in studentRecords.enumerated() {
            DispatchQueue.global().async {
                print("> Writing on thread \(index + 1)")
                XCTAssert(self.localDatabase.write(record))
                expectation.fulfill()
            }
        }
        wait(for: [expectation], timeout: TimeInterval(Self.TIMEOUT))
        // Make sure all records were written
        print("> Counting on main thread")
        XCTAssert(self.localDatabase.count() == expectedCount)
        print("============================== END WRITE THREADS ==================")
    }
    
    func testMultipleReadThreads() throws {
        print("============================== READ THREADS =======================")
        // Setup expectations - XCTest doesn't wait for asynchronous code to complete unless explicitly instructed to do so
        let expectedCount = Self.THREAD_COUNT
        let expectation = XCTestExpectation(description: "Complete all threads")
        expectation.expectedFulfillmentCount = expectedCount
        // Setup records
        var studentRecords = [Record<Student>]()
        for _ in 0..<expectedCount {
            studentRecords.append(Record(data: self.smallStudent))
        }
        // Test case
        for (index, record) in studentRecords.enumerated() {
            DispatchQueue.global().async {
                print("> Writing on thread \(index + 1)")
                XCTAssert(self.localDatabase.write(record))
                print("> Reading many on thread \(index + 1)")
                let read: [Student] = self.localDatabase.read()
                XCTAssertFalse(read.isEmpty)
                print("> Reading IDs on thread \(index + 1)")
                XCTAssertFalse(self.localDatabase.readIDs(Student.self).isEmpty)
                print("> Reading one on thread \(index + 1)")
                let student: Student? = self.localDatabase.read(id: record.metadata.id)
                XCTAssertNotNil(student)
                expectation.fulfill()
            }
        }
        wait(for: [expectation], timeout: TimeInterval(Self.TIMEOUT))
        print("============================== END READ THREADS ===================")
    }
    
    func testMultipleDeleteThreads() throws {
        print("============================== DELETE THREADS =====================")
        // Setup expectations - XCTest doesn't wait for asynchronous code to complete unless explicitly instructed to do so
        let expectedCount = Self.THREAD_COUNT
        let expectation1 = XCTestExpectation(description: "Complete all threads")
        expectation1.expectedFulfillmentCount = expectedCount
        let expectation2 = XCTestExpectation(description: "Complete all threads")
        expectation2.expectedFulfillmentCount = expectedCount
        // Setup records
        var studentRecords = [Record<Student>]()
        for _ in 0..<expectedCount {
            studentRecords.append(Record(data: self.largeStudent))
        }
        // Test case 1
        for (index, record) in studentRecords.enumerated() {
            DispatchQueue.global().async {
                print("> Writing on thread \(index + 1)")
                XCTAssert(self.localDatabase.write(record))
                print("> Deleting one on thread \(index + 1)")
                XCTAssert(self.localDatabase.delete(id: record.metadata.id))
                expectation1.fulfill()
            }
        }
        wait(for: [expectation1], timeout: TimeInterval(Self.TIMEOUT))
        // Test case 2
        for index in 0..<Self.THREAD_COUNT {
            DispatchQueue.global().async {
                print("> Deleting many on thread \(index + 1)")
                let _ = self.localDatabase.delete(Student.self)
                print("> Deleting all on thread \(index + 1)")
                let _ = self.localDatabase.clearDatabase()
                expectation2.fulfill()
            }
        }
        wait(for: [expectation2], timeout: TimeInterval(Self.TIMEOUT))
        print("============================== END DELETE THREADS =================")
    }
    
    func testMultipleCountThreads() throws {
        print("============================== COUNT THREADS ======================")
        // Setup expectations - XCTest doesn't wait for asynchronous code to complete unless explicitly instructed to do so
        let expectedCount = Self.THREAD_COUNT
        let expectation = XCTestExpectation(description: "Complete all threads")
        expectation.expectedFulfillmentCount = expectedCount
        // Setup records
        var studentRecords = [Record<Student>]()
        for _ in 0..<expectedCount {
            studentRecords.append(Record(data: self.largeStudent))
        }
        // Test case
        for (index, record) in studentRecords.enumerated() {
            DispatchQueue.global().async {
                print("> Writing on thread \(index + 1)")
                XCTAssert(self.localDatabase.write(record))
                print("> Counting all on thread \(index + 1)")
                XCTAssert(self.localDatabase.count() > 0)
                print("> Counting on thread \(index + 1)")
                XCTAssert(self.localDatabase.count(Student.self) > 0)
                expectation.fulfill()
            }
        }
        wait(for: [expectation], timeout: TimeInterval(Self.TIMEOUT))
        print("============================== END COUNT THREADS ==================")
    }
    
    func testMultipleTransactionThreads() throws {
        print("============================== TRANSACTION THREADS ================")
        // Setup expectations - XCTest doesn't wait for asynchronous code to complete unless explicitly instructed to do so
        let expectedCount = Self.THREAD_COUNT
        let expectation = XCTestExpectation(description: "Complete all threads")
        expectation.expectedFulfillmentCount = expectedCount
        // Setup records
        var studentRecords = [Record<Student>]()
        for _ in 0..<expectedCount {
            studentRecords.append(Record(data: self.largeStudent))
        }
        // Test case
        for (index, record) in studentRecords.enumerated() {
            DispatchQueue.global().async {
                // Depending on threads, there isn't always a transaction to override (and hence rollback) / commit / rollback
                // We execute a these operations to ensure thread access is safe and valid (otherwise an error is thrown)
                // As to the actual order - this wouldn't be proper code in an application
                // Applications are expected to manage transaction operation order - you shouldn't be starting multiple concurrent transactions simultaneously
                // (That defeats the purpose of being able to access the database from anywhere, and not having to complete a transaction within a block)
                print("> Starting transaction on thread \(index + 1)")
                let _ = self.localDatabase.startTransaction(override: true)
                print("> Writing on thread \(index + 1)")
                XCTAssert(self.localDatabase.write(record))
                print("> Completing transaction on thread \(index + 1)")
                let _ = self.localDatabase.commitTransaction()
                print("> Starting transaction on thread \(index + 1)")
                let _ = self.localDatabase.startTransaction(override: true)
                print("> Writing on thread \(index + 1)")
                XCTAssert(self.localDatabase.write(record))
                print("> Rolling back transaction on thread \(index + 1)")
                let _ = self.localDatabase.rollbackTransaction()
                expectation.fulfill()
            }
        }
        wait(for: [expectation], timeout: TimeInterval(Self.TIMEOUT))
        print("============================== END TRANSACTION THREADS ============")
    }

}
