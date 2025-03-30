//
//  LegacyTests.swift
//  SwiftLocalTests
//
//  Created by Andre Pham on 23/2/2023.
//

import XCTest
@testable import SwiftLocal

final class LegacyTests: XCTestCase {
    
    let localDatabase = try! LocalDatabase()
    
    override func setUp() async throws {
        print("LegacyTests - setUp")
        try await self.localDatabase.clearDatabase()
    }
    
    override func tearDown() async throws {
        print("LegacyTests - tearDown")
        try await self.localDatabase.clearDatabase()
    }
    
    func testFieldAndClassNameRefactor() async throws {
        // First remember to declare the refactor
        Legacy.addClassRefactor(old: "LegacyHomework", new: "Homework")
        
        let legacyHomework = LegacyHomework(legacyAnswers: "1 + 1 = 2", legacyGrade: 100)
        try await self.localDatabase.write(Record(data: legacyHomework))
        let allHomework: [Homework] = try await self.localDatabase.read()
        if allHomework.count == 1 {
            let homework = allHomework[0]
            XCTAssertEqual(homework.answers, legacyHomework.legacyAnswers)
            XCTAssertEqual(homework.grade, legacyHomework.legacyGrade)
        } else {
            XCTFail("Legacy class could not be restored")
        }
    }
    
    func testLegacyReadIDs() async throws {
        // First remember to declare the refactor
        Legacy.addClassRefactor(old: "LegacyHomework", new: "Homework")
        
        let legacyHomework = LegacyHomework(legacyAnswers: "1 + 1 = 2", legacyGrade: 100)
        try await self.localDatabase.write(Record(id: "myHomework", data: legacyHomework))
        let homeworkIDs = try await self.localDatabase.readIDs(Homework.self)
        XCTAssertEqual(homeworkIDs, ["myHomework"])
    }
    
    func testLegacyCount() async throws {
        // First remember to declare the refactor
        Legacy.addClassRefactor(old: "LegacyHomework", new: "Homework")
        
        let legacyHomework = LegacyHomework(legacyAnswers: "1 + 1 = 2", legacyGrade: 100)
        try await self.localDatabase.write(Record(data: legacyHomework))
        let homeworkCount = try await self.localDatabase.count(Homework.self)
        XCTAssertEqual(homeworkCount, 1)
    }
    
    func testLegacyDelete() async throws {
        // First remember to declare the refactor
        Legacy.addClassRefactor(old: "LegacyHomework", new: "Homework")
        
        let legacyHomework = LegacyHomework(legacyAnswers: "1 + 1 = 2", legacyGrade: 100)
        let countBefore = try await self.localDatabase.count()
        XCTAssertEqual(countBefore, 0)
        try await self.localDatabase.write(Record(data: legacyHomework))
        let deletedCount = try await self.localDatabase.delete(Homework.self)
        XCTAssertEqual(deletedCount, 1)
        let countAfter = try await self.localDatabase.count()
        XCTAssertEqual(countAfter, 0)
    }

}
