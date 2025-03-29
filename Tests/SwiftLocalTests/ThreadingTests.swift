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
    let localDatabase = try! LocalDatabase()
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
        try await self.localDatabase.clearDatabase()
    }
    
    override func tearDown() async throws {
        try await self.localDatabase.clearDatabase()
    }
    
    func testMultipleWriteThreads() async throws {
        print("============================== WRITE THREADS ======================")
        let expectedCount = Self.THREAD_COUNT
        // Use a task group to perform concurrent writes
        await withTaskGroup(of: Void.self) { group in
            for index in 0..<expectedCount {
                group.addTask { [weak self] in
                    guard let self = self else { return }
                    let record = Record(data: self.largeStudent)
                    do {
                        print("> Writing on thread \(index + 1)")
                        try await self.localDatabase.write(record)
                    } catch {
                        XCTFail("Write failed on thread \(index + 1): \(error)")
                    }
                }
            }
            // Wait for all tasks to finish
            await group.waitForAll()
        }
        print("> Counting on main thread")
        let count = try await self.localDatabase.count()
        XCTAssertEqual(count, expectedCount)
        print("============================== END WRITE THREADS ==================")
    }
    
    func testMultipleReadThreads() async throws {
        print("============================== READ THREADS =======================")
        let expectedCount = Self.THREAD_COUNT
        await withTaskGroup(of: Void.self) { group in
            for index in 0..<expectedCount {
                group.addTask { [weak self] in
                    guard let self = self else { return }
                    print("> Writing on thread \(index + 1)")
                    let record = Record(data: self.smallStudent)
                    do {
                        print("> Writing on thread \(index + 1)")
                        try await self.localDatabase.write(record)
                        print("> Reading many on thread \(index + 1)")
                        let read: [Student] = try await self.localDatabase.read()
                        XCTAssertFalse(read.isEmpty)
                        print("> Reading IDs on thread \(index + 1)")
                        let ids = try await self.localDatabase.readIDs(Student.self)
                        XCTAssertFalse(ids.isEmpty)
                        print("> Reading one on thread \(index + 1)")
                        let student: Student? = try await self.localDatabase.read(id: record.metadata.id)
                        XCTAssertNotNil(student)
                    } catch {
                        XCTFail("Read thread \(index + 1) failed: \(error)")
                    }
                }
            }
            await group.waitForAll()
        }
        print("============================== END READ THREADS ===================")
    }
    
    func testMultipleDeleteThreads() async throws {
        print("============================== DELETE THREADS =====================")
        let expectedCount = Self.THREAD_COUNT
        // Test case 1: Write and then delete each record by its id
        await withTaskGroup(of: Void.self) { group in
            for index in 0..<expectedCount {
                group.addTask { [weak self] in
                    guard let self = self else { return }
                    let record = Record(data: self.largeStudent)
                    do {
                        print("> Writing on thread \(index + 1)")
                        try await self.localDatabase.write(record)
                        print("> Deleting one on thread \(index + 1)")
                        try await self.localDatabase.delete(id: record.metadata.id)
                    } catch {
                        XCTFail("Delete by id failed on thread \(index + 1): \(error)")
                    }
                }
            }
            await group.waitForAll()
        }
        // Test case 2: Bulk delete tasks (delete by object type and clear database)
        await withTaskGroup(of: Void.self) { group in
            for index in 0..<expectedCount {
                group.addTask { [weak self] in
                    guard let self = self else { return }
                    do {
                        print("> Deleting many on thread \(index + 1)")
                        _ = try await self.localDatabase.delete(Student.self)
                        print("> Deleting all on thread \(index + 1)")
                        _ = try await self.localDatabase.clearDatabase()
                    } catch {
                        XCTFail("Bulk delete failed on thread \(index + 1): \(error)")
                    }
                }
            }
            await group.waitForAll()
        }
        print("============================== END DELETE THREADS =================")
    }
    
    func testMultipleCountThreads() async throws {
        print("============================== COUNT THREADS ======================")
        let expectedCount = Self.THREAD_COUNT
        await withTaskGroup(of: Void.self) { group in
            for index in 0..<expectedCount {
                group.addTask { [weak self] in
                    guard let self = self else { return }
                    let record = Record(data: self.largeStudent)
                    do {
                        print("> Writing on thread \(index + 1)")
                        try await self.localDatabase.write(record)
                        print("> Counting all on thread \(index + 1)")
                        let totalCount = try await self.localDatabase.count()
                        XCTAssertTrue(totalCount > 0)
                        print("> Counting on thread \(index + 1)")
                        let countForStudent = try await self.localDatabase.count(Student.self)
                        XCTAssertTrue(countForStudent > 0)
                    } catch {
                        XCTFail("Count failed on thread \(index + 1): \(error)")
                    }
                }
            }
            await group.waitForAll()
        }
        print("============================== END COUNT THREADS ==================")
    }
    
    func testMultipleTransactionThreads() async throws {
        print("============================== TRANSACTION THREADS ================")
        let expectedCount = Self.THREAD_COUNT
        await withTaskGroup(of: Void.self) { group in
            for index in 0..<expectedCount {
                group.addTask { [weak self] in
                    guard let self = self else { return }
                    let record = Record(data: self.largeStudent)
                    do {
                        // Depending on threads, there isn't always a transaction to override (and hence rollback) / commit / rollback
                        // We execute these operations to ensure thread access is safe and valid (otherwise an error is thrown)
                        // As to the actual order - this wouldn't be proper code in an application
                        // Applications are expected to manage transaction operation order - you shouldn't be starting multiple concurrent transactions simultaneously
                        print("> Starting transaction on thread \(index + 1)")
                        try await self.localDatabase.startTransaction(override: true)
                        print("> Writing on thread \(index + 1)")
                        try await self.localDatabase.write(record)
                        print("> Committing transaction on thread \(index + 1)")
                        try await self.localDatabase.commitTransaction()
                        print("> Starting second transaction on thread \(index + 1)")
                        try await self.localDatabase.startTransaction(override: true)
                        print("> Writing on thread \(index + 1)")
                        try await self.localDatabase.write(record)
                        print("> Rolling back transaction on thread \(index + 1)")
                        try await self.localDatabase.rollbackTransaction()
                    } catch {
                        XCTFail("Transaction failed on thread \(index + 1): \(error)")
                    }
                }
            }
            await group.waitForAll()
        }
        print("============================== END TRANSACTION THREADS =================")
    }

}
