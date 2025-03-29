//
//  DatabaseTargetTests.swift
//  SwiftLocalTests
//
//  Created by Andre Pham on 23/2/2023.
//

import XCTest
@testable import SwiftLocal

final class DatabaseTargetTests: XCTestCase {

    let databaseTargets: [DatabaseTarget] = [LocalDatabase()]
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
        self.databaseTargets.forEach({ _ = $0.clearDatabase() })
    }
    
    override func tearDown() {
        self.databaseTargets.forEach({ _ = $0.clearDatabase() })
    }

    func testWrite() throws {
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            let record = Record(data: self.student1)
            XCTAssert(database.write(record))
            XCTAssert(database.count() == 1)
        }
    }
    
    func testReadByObjectType() throws {
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            XCTAssert(database.write(Record(data: self.student1)))
            XCTAssert(database.write(Record(data: self.student2)))
            let readStudents: [Student] = database.read()
            XCTAssertEqual(readStudents.count, 2)
            XCTAssert(readStudents.contains(where: { $0.firstName == self.student1.firstName }))
            XCTAssert(readStudents.contains(where: { $0.firstName == self.student2.firstName }))
            XCTAssert(database.count() == 2)
        }
    }
    
    func testReadByID() throws {
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            let record = Record(id: "testID", data: self.student1)
            XCTAssert(database.write(record))
            let readStudent: Student? = database.read(id: "testID")
            XCTAssertNotNil(readStudent)
            XCTAssert(database.count() == 1)
        }
    }
    
    func testReadIDs() throws {
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            XCTAssert(database.write(Record(id: "testID1", data: self.student1)))
            XCTAssert(database.write(Record(id: "testID2", data: self.student2)))
            XCTAssert(database.write(Record(id: "testID3", data: self.teacher)))
            let studentIDs = database.readIDs(Student.self)
            let teacherIDs = database.readIDs(Teacher.self)
            XCTAssert(studentIDs.contains("testID1"))
            XCTAssert(studentIDs.contains("testID2"))
            XCTAssert(studentIDs.count == 2)
            XCTAssert(teacherIDs.contains("testID3"))
            XCTAssert(teacherIDs.count == 1)
        }
    }
    
    func testDeleteByObjectType() throws {
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            XCTAssert(database.write(Record(data: self.student1)))
            XCTAssert(database.write(Record(data: self.student2)))
            XCTAssert(database.write(Record(data: self.teacher)))
            let countDeleted = database.delete(Student.self)
            XCTAssertEqual(countDeleted, 2)
            let readStudents: [Student] = database.read()
            XCTAssertEqual(readStudents.count, 0)
            let readTeachers: [Teacher] = database.read()
            XCTAssertEqual(readTeachers.count, 1)
            XCTAssert(database.count() == 1)
        }
    }
    
    func testDeleteByID() throws {
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            XCTAssert(database.write(Record(id: "student1", data: self.student1)))
            XCTAssert(database.write(Record(id: "student2", data: self.student2)))
            XCTAssert(database.delete(id: "student1"))
            let readStudent1: Student? = database.read(id: "student1")
            let readStudent2: Student? = database.read(id: "student2")
            XCTAssertNil(readStudent1)
            XCTAssertNotNil(readStudent2)
            XCTAssert(database.count() == 1)
        }
    }
    
    func testClearDatabase() throws {
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            XCTAssert(database.write(Record(id: "student1", data: self.student1)))
            XCTAssert(database.write(Record(id: "student2", data: self.student2)))
            XCTAssertEqual(database.clearDatabase(), 2)
            let readStudents: [Student] = database.read()
            XCTAssertEqual(readStudents.count, 0)
            XCTAssert(database.count() == 0)
        }
    }
    
    func testReplace() throws {
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            XCTAssert(database.write(Record(id: "student", data: self.student1)))
            XCTAssert(database.write(Record(id: "student", data: self.student2)))
            let readStudent: Student? = database.read(id: "student")
            XCTAssertEqual(readStudent?.firstName, self.student2.firstName)
            XCTAssert(database.count() == 1)
        }
    }
    
    func testCount() throws {
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            XCTAssert(database.write(Record(data: self.student1)))
            XCTAssert(database.write(Record(data: self.student2)))
            XCTAssert(database.write(Record(data: self.teacher)))
            XCTAssert(database.count() == 3)
            XCTAssert(database.count(Student.self) == 2)
            XCTAssert(database.count(Teacher.self) == 1)
        }
    }

}
