//
//  LocalDatabaseTests.swift
//  SwiftLocalTests
//
//  Created by Andre Pham on 23/2/2023.
//

import XCTest
@testable import SwiftLocal

internal final class LocalDatabaseTests: XCTestCase {
    // MARK: Properties

    internal let localDatabase = try! LocalDatabase()

    // MARK: Computed Properties

    internal var student1: Student {
        Student(firstName: "Billy", lastName: "Bob", debt: 100_000.0, teacher: self.teacher, subjectNames: ["Physics", "English"])
    }

    internal var student2: Student {
        Student(firstName: "Sammy", lastName: "Sob", debt: 0.0, teacher: self.teacher, subjectNames: ["Math"])
    }

    internal var teacher: Teacher {
        Teacher(firstName: "Karen", lastName: "Kob", salary: 50_000.0)
    }

    // MARK: Overridden Functions

    internal override func setUp() async throws {
        try await self.localDatabase.clearDatabase()
    }

    internal override func tearDown() async throws {
        try await self.localDatabase.clearDatabase()
    }

    // MARK: Functions

    internal func testWrite() async throws {
        let record = Record(data: self.student1)
        try await self.localDatabase.write(record)
        let count = try await self.localDatabase.count()
        XCTAssertEqual(count, 1)
    }

    internal func testReadByObjectType() async throws {
        try await self.localDatabase.write(Record(data: self.student1))
        try await self.localDatabase.write(Record(data: self.student2))
        let readStudents: [Student] = try await self.localDatabase.read()
        XCTAssertEqual(readStudents.count, 2)
        XCTAssertTrue(readStudents.contains { $0.firstName == self.student1.firstName })
        XCTAssertTrue(readStudents.contains { $0.firstName == self.student2.firstName })
        let count = try await self.localDatabase.count()
        XCTAssertEqual(count, 2)
    }

    internal func testReadByID() async throws {
        let record = Record(id: "testID", data: self.student1)
        try await self.localDatabase.write(record)
        let readStudent: Student? = try await self.localDatabase.read(id: "testID")
        XCTAssertNotNil(readStudent)
        let count = try await self.localDatabase.count()
        XCTAssertEqual(count, 1)
    }

    internal func testReadIDs() async throws {
        try await self.localDatabase.write(Record(id: "testID1", data: self.student1))
        try await self.localDatabase.write(Record(id: "testID2", data: self.student2))
        try await self.localDatabase.write(Record(id: "testID3", data: self.teacher))
        let studentIDs = try await self.localDatabase.readIDs(Student.self)
        let teacherIDs = try await self.localDatabase.readIDs(Teacher.self)
        XCTAssertTrue(studentIDs.contains("testID1"))
        XCTAssertTrue(studentIDs.contains("testID2"))
        XCTAssertEqual(studentIDs.count, 2)
        XCTAssertTrue(teacherIDs.contains("testID3"))
        XCTAssertEqual(teacherIDs.count, 1)
    }

    internal func testDeleteByObjectType() async throws {
        try await self.localDatabase.write(Record(data: self.student1))
        try await self.localDatabase.write(Record(data: self.student2))
        try await self.localDatabase.write(Record(data: self.teacher))
        let countDeleted = try await self.localDatabase.delete(Student.self)
        XCTAssertEqual(countDeleted, 2)
        let readStudents: [Student] = try await self.localDatabase.read()
        XCTAssertEqual(readStudents.count, 0)
        let readTeachers: [Teacher] = try await self.localDatabase.read()
        XCTAssertEqual(readTeachers.count, 1)
        let count = try await self.localDatabase.count()
        XCTAssertEqual(count, 1)
    }

    internal func testDeleteByID() async throws {
        try await self.localDatabase.write(Record(id: "student1", data: self.student1))
        try await self.localDatabase.write(Record(id: "student2", data: self.student2))
        try await self.localDatabase.delete(id: "student1")
        let readStudent1: Student? = try await self.localDatabase.read(id: "student1")
        let readStudent2: Student? = try await self.localDatabase.read(id: "student2")
        XCTAssertNil(readStudent1)
        XCTAssertNotNil(readStudent2)
        let count = try await self.localDatabase.count()
        XCTAssertEqual(count, 1)
    }

    internal func testClearDatabase() async throws {
        try await self.localDatabase.write(Record(id: "student1", data: self.student1))
        try await self.localDatabase.write(Record(id: "student2", data: self.student2))
        let countDeleted = try await self.localDatabase.clearDatabase()
        XCTAssertEqual(countDeleted, 2)
        let readStudents: [Student] = try await self.localDatabase.read()
        XCTAssertEqual(readStudents.count, 0)
        let count = try await self.localDatabase.count()
        XCTAssertEqual(count, 0)
    }

    internal func testReplace() async throws {
        try await self.localDatabase.write(Record(id: "student", data: self.student1))
        try await self.localDatabase.write(Record(id: "student", data: self.student2))
        let readStudent: Student? = try await self.localDatabase.read(id: "student")
        XCTAssertEqual(readStudent?.firstName, self.student2.firstName)
        let count = try await self.localDatabase.count()
        XCTAssertEqual(count, 1)
    }

    internal func testCount() async throws {
        try await self.localDatabase.write(Record(data: self.student1))
        try await self.localDatabase.write(Record(data: self.student2))
        try await self.localDatabase.write(Record(data: self.teacher))
        let countAll = try await self.localDatabase.count()
        XCTAssertEqual(countAll, 3)
        let studentCount = try await self.localDatabase.count(Student.self)
        XCTAssertEqual(studentCount, 2)
        let teacherCount = try await self.localDatabase.count(Teacher.self)
        XCTAssertEqual(teacherCount, 1)
    }
}
