//
//  DatabaseTargetTests.swift
//  SwiftLocalTests
//
//  Created by Andre Pham on 23/2/2023.
//

import XCTest
@testable import SwiftLocal

final class DatabaseTargetTests: XCTestCase {

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

    func testWrite() throws {
        let record = Record(data: self.student1)
        XCTAssert(self.localDatabase.write(record))
        XCTAssert(self.localDatabase.count() == 1)
    }
    
    func testReadByObjectType() throws {
        XCTAssert(self.localDatabase.write(Record(data: self.student1)))
        XCTAssert(self.localDatabase.write(Record(data: self.student2)))
        let readStudents: [Student] = self.localDatabase.read()
        XCTAssertEqual(readStudents.count, 2)
        XCTAssert(readStudents.contains(where: { $0.firstName == self.student1.firstName }))
        XCTAssert(readStudents.contains(where: { $0.firstName == self.student2.firstName }))
        XCTAssert(self.localDatabase.count() == 2)
    }
    
    func testReadByID() throws {
        let record = Record(id: "testID", data: self.student1)
        XCTAssert(self.localDatabase.write(record))
        let readStudent: Student? = self.localDatabase.read(id: "testID")
        XCTAssertNotNil(readStudent)
        XCTAssert(self.localDatabase.count() == 1)
    }
    
    func testReadIDs() throws {
        XCTAssert(self.localDatabase.write(Record(id: "testID1", data: self.student1)))
        XCTAssert(self.localDatabase.write(Record(id: "testID2", data: self.student2)))
        XCTAssert(self.localDatabase.write(Record(id: "testID3", data: self.teacher)))
        let studentIDs = self.localDatabase.readIDs(Student.self)
        let teacherIDs = self.localDatabase.readIDs(Teacher.self)
        XCTAssert(studentIDs.contains("testID1"))
        XCTAssert(studentIDs.contains("testID2"))
        XCTAssert(studentIDs.count == 2)
        XCTAssert(teacherIDs.contains("testID3"))
        XCTAssert(teacherIDs.count == 1)
    }
    
    func testDeleteByObjectType() throws {
        XCTAssert(self.localDatabase.write(Record(data: self.student1)))
        XCTAssert(self.localDatabase.write(Record(data: self.student2)))
        XCTAssert(self.localDatabase.write(Record(data: self.teacher)))
        let countDeleted = self.localDatabase.delete(Student.self)
        XCTAssertEqual(countDeleted, 2)
        let readStudents: [Student] = self.localDatabase.read()
        XCTAssertEqual(readStudents.count, 0)
        let readTeachers: [Teacher] = self.localDatabase.read()
        XCTAssertEqual(readTeachers.count, 1)
        XCTAssert(self.localDatabase.count() == 1)
    }
    
    func testDeleteByID() throws {
        XCTAssert(self.localDatabase.write(Record(id: "student1", data: self.student1)))
        XCTAssert(self.localDatabase.write(Record(id: "student2", data: self.student2)))
        XCTAssert(self.localDatabase.delete(id: "student1"))
        let readStudent1: Student? = self.localDatabase.read(id: "student1")
        let readStudent2: Student? = self.localDatabase.read(id: "student2")
        XCTAssertNil(readStudent1)
        XCTAssertNotNil(readStudent2)
        XCTAssert(self.localDatabase.count() == 1)
    }
    
    func testClearDatabase() throws {
        XCTAssert(self.localDatabase.write(Record(id: "student1", data: self.student1)))
        XCTAssert(self.localDatabase.write(Record(id: "student2", data: self.student2)))
        XCTAssertEqual(self.localDatabase.clearDatabase(), 2)
        let readStudents: [Student] = self.localDatabase.read()
        XCTAssertEqual(readStudents.count, 0)
        XCTAssert(self.localDatabase.count() == 0)
    }
    
    func testReplace() throws {
        XCTAssert(self.localDatabase.write(Record(id: "student", data: self.student1)))
        XCTAssert(self.localDatabase.write(Record(id: "student", data: self.student2)))
        let readStudent: Student? = self.localDatabase.read(id: "student")
        XCTAssertEqual(readStudent?.firstName, self.student2.firstName)
        XCTAssert(self.localDatabase.count() == 1)
    }
    
    func testCount() throws {
        XCTAssert(self.localDatabase.write(Record(data: self.student1)))
        XCTAssert(self.localDatabase.write(Record(data: self.student2)))
        XCTAssert(self.localDatabase.write(Record(data: self.teacher)))
        XCTAssert(self.localDatabase.count() == 3)
        XCTAssert(self.localDatabase.count(Student.self) == 2)
        XCTAssert(self.localDatabase.count(Teacher.self) == 1)
    }

}
