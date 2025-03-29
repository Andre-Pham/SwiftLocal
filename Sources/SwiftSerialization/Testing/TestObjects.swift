//
//  TestObjects.swift
//  SwiftSerialization
//
//  Created by Andre Pham on 23/2/2023.
//

import Foundation

internal class Person: Storable {
    
    private(set) var firstName: String
    private(set) var lastName: String
    public let id = UUID()
    
    init(firstName: String, lastName: String) {
        self.firstName = firstName
        self.lastName = lastName
    }
    
    // MARK: - Serialization
    
    private enum Field: String {
        case firstName
        case lastName
    }
    
    required init(dataObject: DataObject) {
        self.firstName = dataObject.get(Field.firstName.rawValue)
        self.lastName = dataObject.get(Field.lastName.rawValue)
    }
    
    func toDataObject() -> DataObject {
        return DataObject(self)
            .add(key: Field.firstName.rawValue, value: self.firstName)
            .add(key: Field.lastName.rawValue, value: self.lastName)
    }
    
}

internal class Student: Person {
    
    private(set) var homework = [Homework]()
    private(set) var debt: Double
    private(set) var teacher: Teacher
    private(set) var subjectNames: [String]
    
    init(firstName: String, lastName: String, debt: Double, teacher: Teacher, subjectNames: [String]) {
        self.debt = debt
        self.teacher = teacher
        self.subjectNames = subjectNames
        super.init(firstName: firstName, lastName: lastName)
    }
    
    func giveHomework(_ homework: Homework) {
        self.homework.append(homework)
    }
    
    // MARK: - Serialization
    
    private enum Field: String {
        case debt
        case homework
        case teacher
        case subjectNames
    }
    
    required init(dataObject: DataObject) {
        self.debt = dataObject.get(Field.debt.rawValue)
        self.homework = dataObject.getObjectArray(Field.homework.rawValue, type: Homework.self)
        self.teacher = dataObject.getObject(Field.teacher.rawValue, type: Teacher.self)
        self.subjectNames = dataObject.get(Field.subjectNames.rawValue)
        super.init(dataObject: dataObject)
    }
    
    override func toDataObject() -> DataObject {
        return super.toDataObject()
            .add(key: Field.debt.rawValue, value: self.debt)
            .add(key: Field.homework.rawValue, value: self.homework)
            .add(key: Field.teacher.rawValue, value: self.teacher)
            .add(key: Field.subjectNames.rawValue, value: self.subjectNames)
    }
    
}

internal class Teacher: Person {
    
    private(set) var salary: Double
    
    init(firstName: String, lastName: String, salary: Double) {
        self.salary = salary
        super.init(firstName: firstName, lastName: lastName)
    }
    
    // MARK: - Serialization
    
    private enum Field: String {
        case salary
    }
    
    required init(dataObject: DataObject) {
        self.salary = dataObject.get(Field.salary.rawValue)
        super.init(dataObject: dataObject)
    }
    
    override func toDataObject() -> DataObject {
        return super.toDataObject()
            .add(key: Field.salary.rawValue, value: self.salary)
    }
    
}

internal class Homework: Storable {
    
    public let answers: String
    private(set) var grade: Int?
    
    init(answers: String, grade: Int?) {
        self.answers = answers
        self.grade = grade
    }
    
    // MARK: - Serialization
    
    private enum Field: String {
        case answers
        case grade
    }
    
    required init(dataObject: DataObject) {
        self.answers = dataObject.get(Field.answers.rawValue, legacyKeys: ["legacyAnswers"])
        self.grade = dataObject.get(Field.grade.rawValue, legacyKeys: ["legacyGrade"])
    }
    
    func toDataObject() -> DataObject {
        return DataObject(self)
            .add(key: Field.answers.rawValue, value: self.answers)
            .add(key: Field.grade.rawValue, value: self.grade)
    }
    
}

/// This is to test legacy support. It is identical to the Homework class, but with a different class name and attribute names.
/// Homework has legacy keys added to its init(dataObject: DataObject) so if legacy support is implemented correctly, you should be able to save an instance of LegacyHomework and restore it as Homework.
internal class LegacyHomework: Storable {
    
    public let legacyAnswers: String
    private(set) var legacyGrade: Int?
    
    init(legacyAnswers: String, legacyGrade: Int?) {
        self.legacyAnswers = legacyAnswers
        self.legacyGrade = legacyGrade
    }
    
    // MARK: - Serialization
    
    private enum Field: String {
        case legacyAnswers
        case legacyGrade
    }
    
    required init(dataObject: DataObject) {
        self.legacyAnswers = dataObject.get(Field.legacyAnswers.rawValue)
        self.legacyGrade = dataObject.get(Field.legacyGrade.rawValue)
    }
    
    func toDataObject() -> DataObject {
        return DataObject(self)
            .add(key: Field.legacyAnswers.rawValue, value: self.legacyAnswers)
            .add(key: Field.legacyGrade.rawValue, value: self.legacyGrade)
    }
    
}
