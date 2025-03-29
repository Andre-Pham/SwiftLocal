//
//  LegacyTests.swift
//  SwiftLocalTests
//
//  Created by Andre Pham on 23/2/2023.
//

import XCTest
@testable import SwiftLocal

final class LegacyTests: XCTestCase {
    
    let databaseTargets: [DatabaseTarget] = [SerializationDatabase()]
    
    override func setUp() async throws {
        self.databaseTargets.forEach({ _ = $0.clearDatabase() })
    }
    
    override func tearDown() {
        self.databaseTargets.forEach({ _ = $0.clearDatabase() })
    }

    func testFieldAndClassNameRefactor() throws {
        // First remember to declare the refactor
        Legacy.addClassRefactor(old: "LegacyHomework", new: "Homework")
        
        let legacyHomework = LegacyHomework(legacyAnswers: "1 + 1 = 2", legacyGrade: 100)
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            XCTAssert(database.write(Record(data: legacyHomework)))
            let allHomework: [Homework] = database.read()
            if allHomework.count == 1 {
                let homework = allHomework[0]
                XCTAssertEqual(homework.answers, legacyHomework.legacyAnswers)
                XCTAssertEqual(homework.grade, legacyHomework.legacyGrade)
            } else {
                XCTFail("Legacy class could not be restored")
            }
        }
    }
    
    func testLegacyReadIDs() throws {
        // First remember to declare the refactor
        Legacy.addClassRefactor(old: "LegacyHomework", new: "Homework")
        
        let legacyHomework = LegacyHomework(legacyAnswers: "1 + 1 = 2", legacyGrade: 100)
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            XCTAssert(database.write(Record(id: "myHomework", data: legacyHomework)))
            let homeworkCount = database.readIDs(Homework.self)
            XCTAssertEqual(homeworkCount, ["myHomework"])
        }
    }
    
    func testLegacyCount() throws {
        // First remember to declare the refactor
        Legacy.addClassRefactor(old: "LegacyHomework", new: "Homework")
        
        let legacyHomework = LegacyHomework(legacyAnswers: "1 + 1 = 2", legacyGrade: 100)
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            XCTAssert(database.write(Record(data: legacyHomework)))
            let homeworkCount = database.count(Homework.self)
            XCTAssertEqual(homeworkCount, 1)
        }
    }
    
    func testLegacyDelete() throws {
        // First remember to declare the refactor
        Legacy.addClassRefactor(old: "LegacyHomework", new: "Homework")
        
        let legacyHomework = LegacyHomework(legacyAnswers: "1 + 1 = 2", legacyGrade: 100)
        for database in self.databaseTargets {
            print("-- DATABASE \(database.self) --")
            
            XCTAssert(database.count() == 0)
            XCTAssert(database.write(Record(data: legacyHomework)))
            XCTAssert(database.delete(Homework.self) == 1)
            XCTAssert(database.count() == 0)
        }
    }

}
