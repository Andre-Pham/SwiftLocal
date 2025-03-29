//
//  LegacyTests.swift
//  SwiftLocalTests
//
//  Created by Andre Pham on 23/2/2023.
//

import XCTest
@testable import SwiftLocal

final class LegacyTests: XCTestCase {
    
    let localDatabase = LocalDatabase()
    
    override func setUp() async throws {
        self.localDatabase.clearDatabase()
    }
    
    override func tearDown() {
        self.localDatabase.clearDatabase()
    }

    func testFieldAndClassNameRefactor() throws {
        // First remember to declare the refactor
        Legacy.addClassRefactor(old: "LegacyHomework", new: "Homework")
        
        let legacyHomework = LegacyHomework(legacyAnswers: "1 + 1 = 2", legacyGrade: 100)
        XCTAssert(self.localDatabase.write(Record(data: legacyHomework)))
        let allHomework: [Homework] = self.localDatabase.read()
        if allHomework.count == 1 {
            let homework = allHomework[0]
            XCTAssertEqual(homework.answers, legacyHomework.legacyAnswers)
            XCTAssertEqual(homework.grade, legacyHomework.legacyGrade)
        } else {
            XCTFail("Legacy class could not be restored")
        }
    }
    
    func testLegacyReadIDs() throws {
        // First remember to declare the refactor
        Legacy.addClassRefactor(old: "LegacyHomework", new: "Homework")
        
        let legacyHomework = LegacyHomework(legacyAnswers: "1 + 1 = 2", legacyGrade: 100)
        XCTAssert(self.localDatabase.write(Record(id: "myHomework", data: legacyHomework)))
        let homeworkCount = self.localDatabase.readIDs(Homework.self)
        XCTAssertEqual(homeworkCount, ["myHomework"])
    }
    
    func testLegacyCount() throws {
        // First remember to declare the refactor
        Legacy.addClassRefactor(old: "LegacyHomework", new: "Homework")
        
        let legacyHomework = LegacyHomework(legacyAnswers: "1 + 1 = 2", legacyGrade: 100)
        XCTAssert(self.localDatabase.write(Record(data: legacyHomework)))
        let homeworkCount = self.localDatabase.count(Homework.self)
        XCTAssertEqual(homeworkCount, 1)
    }
    
    func testLegacyDelete() throws {
        // First remember to declare the refactor
        Legacy.addClassRefactor(old: "LegacyHomework", new: "Homework")
        
        let legacyHomework = LegacyHomework(legacyAnswers: "1 + 1 = 2", legacyGrade: 100)
        XCTAssert(self.localDatabase.count() == 0)
        XCTAssert(self.localDatabase.write(Record(data: legacyHomework)))
        XCTAssert(self.localDatabase.delete(Homework.self) == 1)
        XCTAssert(self.localDatabase.count() == 0)
    }

}
