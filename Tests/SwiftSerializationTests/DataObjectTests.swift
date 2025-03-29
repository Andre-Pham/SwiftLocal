//
//  DataObjectTests.swift
//  SwiftLocalTests
//
//  Created by Andre Pham on 23/2/2023.
//

import XCTest
@testable import SwiftLocal

final class DataObjectTests: XCTestCase {
    
    let databaseTargets: [DatabaseTarget] = [SerializationDatabase()]

    let student = Student(
        firstName: "Billy",
        lastName: "Bob",
        debt: 100_000.0,
        teacher: Teacher(firstName: "Karen", lastName: "Kob", salary: 50_000.0),
        subjectNames: ["Physics", "English"]
    )
    
    override func setUp() async throws {
        self.databaseTargets.forEach({ _ = $0.clearDatabase() })
        self.student.giveHomework(Homework(answers: "2x + 5", grade: nil))
        self.student.giveHomework(Homework(answers: "Something smart", grade: 99))
    }
    
    override func tearDown() {
        self.databaseTargets.forEach({ _ = $0.clearDatabase() })
    }
    
    func testSerialization() throws {
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            XCTAssertTrue(database.write(Record(id: "student", data: self.student)))
            let readStudent: Student? = database.read(id: "student")
            XCTAssertNotNil(readStudent)
            
            // Make sure all data is correctly saved and restored
            XCTAssertEqual(self.student.firstName, readStudent?.firstName)
            XCTAssertEqual(self.student.lastName, readStudent?.lastName)
            XCTAssertEqual(self.student.debt, readStudent?.debt)
            XCTAssertEqual(self.student.teacher.firstName, readStudent?.teacher.firstName)
            XCTAssertEqual(self.student.teacher.lastName, readStudent?.teacher.lastName)
            XCTAssertEqual(self.student.teacher.salary, readStudent?.teacher.salary)
            XCTAssertEqual(self.student.homework.count, readStudent?.homework.count)
            XCTAssertEqual(self.student.homework.first?.answers, readStudent?.homework.first?.answers)
            XCTAssertEqual(self.student.homework.last?.answers, readStudent?.homework.last?.answers)
            XCTAssertEqual(self.student.homework.first?.grade, readStudent?.homework.first?.grade)
            XCTAssertEqual(self.student.homework.last?.grade, readStudent?.homework.last?.grade)
            XCTAssertEqual(self.student.subjectNames, readStudent?.subjectNames)
        }
    }
    
    func testRawSeralization() throws {
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            // 1. Convert to raw string
            let studentDataObject = self.student.toDataObject()
            let studentSerialized = studentDataObject.toRawString()
            // 2. Convert raw string back into student
            let readStudent = DataObject(rawString: studentSerialized!).restore(Student.self)
            
            // Make sure all data is correctly saved and restored
            XCTAssertEqual(self.student.firstName, readStudent.firstName)
            XCTAssertEqual(self.student.lastName, readStudent.lastName)
            XCTAssertEqual(self.student.debt, readStudent.debt)
            XCTAssertEqual(self.student.teacher.firstName, readStudent.teacher.firstName)
            XCTAssertEqual(self.student.teacher.lastName, readStudent.teacher.lastName)
            XCTAssertEqual(self.student.teacher.salary, readStudent.teacher.salary)
            XCTAssertEqual(self.student.homework.count, readStudent.homework.count)
            XCTAssertEqual(self.student.homework.first?.answers, readStudent.homework.first?.answers)
            XCTAssertEqual(self.student.homework.last?.answers, readStudent.homework.last?.answers)
            XCTAssertEqual(self.student.homework.first?.grade, readStudent.homework.first?.grade)
            XCTAssertEqual(self.student.homework.last?.grade, readStudent.homework.last?.grade)
            XCTAssertEqual(self.student.subjectNames, readStudent.subjectNames)
        }
    }

}
