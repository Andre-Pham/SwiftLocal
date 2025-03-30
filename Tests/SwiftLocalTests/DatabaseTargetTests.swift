//
//  DatabaseTargetTests.swift
//  SwiftLocalTests
//
//  Created by Andre Pham on 23/2/2023.
//

import XCTest
@testable import SwiftLocal

final class DatabaseTargetTests: XCTestCase {

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
        try await self.localDatabase.clearDatabase()
    }
    
    override func tearDown() async throws {
        try await self.localDatabase.clearDatabase()
    }

    func testWrite() async throws {
        let record = Record(data: self.student1)
        try await self.localDatabase.write(record)
        let count = try await self.localDatabase.count()
        XCTAssertEqual(count, 1)
    }
    
    func testReadByObjectType() async throws {
        try await self.localDatabase.write(Record(data: self.student1))
        try await self.localDatabase.write(Record(data: self.student2))
        let readStudents: [Student] = try await self.localDatabase.read()
        XCTAssertEqual(readStudents.count, 2)
        XCTAssertTrue(readStudents.contains { $0.firstName == student1.firstName })
        XCTAssertTrue(readStudents.contains { $0.firstName == student2.firstName })
        let count = try await self.localDatabase.count()
        XCTAssertEqual(count, 2)
    }
    
    func testReadByID() async throws {
        let record = Record(id: "testID", data: self.student1)
        try await self.localDatabase.write(record)
        let readStudent: Student? = try await self.localDatabase.read(id: "testID")
        XCTAssertNotNil(readStudent)
        let count = try await self.localDatabase.count()
        XCTAssertEqual(count, 1)
    }
    
    func testReadIDs() async throws {
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
    
    func testDeleteByObjectType() async throws {
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
    
    func testDeleteByID() async throws {
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
    
    func testClearDatabase() async throws {
        try await self.localDatabase.write(Record(id: "student1", data: self.student1))
        try await self.localDatabase.write(Record(id: "student2", data: self.student2))
        let countDeleted = try await self.localDatabase.clearDatabase()
        XCTAssertEqual(countDeleted, 2)
        let readStudents: [Student] = try await self.localDatabase.read()
        XCTAssertEqual(readStudents.count, 0)
        let count = try await self.localDatabase.count()
        XCTAssertEqual(count, 0)
    }
    
    func testReplace() async throws {
        try await self.localDatabase.write(Record(id: "student", data: self.student1))
        try await self.localDatabase.write(Record(id: "student", data: self.student2))
        let readStudent: Student? = try await self.localDatabase.read(id: "student")
        XCTAssertEqual(readStudent?.firstName, student2.firstName)
        let count = try await self.localDatabase.count()
        XCTAssertEqual(count, 1)
    }
    
    func testCount() async throws {
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
