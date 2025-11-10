//
//  TestObjects.swift
//  SwiftLocal
//
//  Created by Andre Pham on 23/2/2023.
//

import Foundation

internal class Person: Storable {
    // MARK: Nested Types

    private enum Field: String {
        case firstName
        case lastName
    }

    // MARK: Properties

    internal let id = UUID()

    internal private(set) var firstName: String
    internal private(set) var lastName: String

    // MARK: Lifecycle

    internal init(firstName: String, lastName: String) {
        self.firstName = firstName
        self.lastName = lastName
    }

    internal required init(dataObject: DataObject) {
        self.firstName = dataObject.get(Field.firstName.rawValue)
        self.lastName = dataObject.get(Field.lastName.rawValue)
    }

    // MARK: Functions

    internal func toDataObject() -> DataObject {
        return DataObject(self)
            .add(key: Field.firstName.rawValue, value: self.firstName)
            .add(key: Field.lastName.rawValue, value: self.lastName)
    }
}

internal class Student: Person {
    // MARK: Nested Types

    private enum Field: String {
        case debt
        case homework
        case teacher
        case subjectNames
    }

    // MARK: Properties

    internal private(set) var homework = [Homework]()
    internal private(set) var debt: Double
    internal private(set) var teacher: Teacher
    internal private(set) var subjectNames: [String]

    // MARK: Lifecycle

    internal init(firstName: String, lastName: String, debt: Double, teacher: Teacher, subjectNames: [String]) {
        self.debt = debt
        self.teacher = teacher
        self.subjectNames = subjectNames
        super.init(firstName: firstName, lastName: lastName)
    }

    internal required init(dataObject: DataObject) {
        self.debt = dataObject.get(Field.debt.rawValue)
        self.homework = dataObject.getObjectArray(Field.homework.rawValue, type: Homework.self)
        self.teacher = dataObject.getObject(Field.teacher.rawValue, type: Teacher.self)
        self.subjectNames = dataObject.get(Field.subjectNames.rawValue)
        super.init(dataObject: dataObject)
    }

    // MARK: Overridden Functions

    internal override func toDataObject() -> DataObject {
        return super.toDataObject()
            .add(key: Field.debt.rawValue, value: self.debt)
            .add(key: Field.homework.rawValue, value: self.homework)
            .add(key: Field.teacher.rawValue, value: self.teacher)
            .add(key: Field.subjectNames.rawValue, value: self.subjectNames)
    }

    // MARK: Functions

    internal func giveHomework(_ homework: Homework) {
        self.homework.append(homework)
    }
}

internal class Teacher: Person {
    // MARK: Nested Types

    private enum Field: String {
        case salary
    }

    // MARK: Properties

    internal private(set) var salary: Double

    // MARK: Lifecycle

    internal init(firstName: String, lastName: String, salary: Double) {
        self.salary = salary
        super.init(firstName: firstName, lastName: lastName)
    }

    internal required init(dataObject: DataObject) {
        self.salary = dataObject.get(Field.salary.rawValue)
        super.init(dataObject: dataObject)
    }

    // MARK: Overridden Functions

    internal override func toDataObject() -> DataObject {
        return super.toDataObject()
            .add(key: Field.salary.rawValue, value: self.salary)
    }
}

internal class Homework: Storable {
    // MARK: Nested Types

    private enum Field: String {
        case answers
        case grade
    }

    // MARK: Properties

    internal let answers: String

    internal private(set) var grade: Int?

    // MARK: Lifecycle

    internal init(answers: String, grade: Int?) {
        self.answers = answers
        self.grade = grade
    }

    internal required init(dataObject: DataObject) {
        self.answers = dataObject.get(Field.answers.rawValue, legacyKeys: ["legacyAnswers"])
        self.grade = dataObject.get(Field.grade.rawValue, legacyKeys: ["legacyGrade"])
    }

    // MARK: Functions

    internal func toDataObject() -> DataObject {
        return DataObject(self)
            .add(key: Field.answers.rawValue, value: self.answers)
            .add(key: Field.grade.rawValue, value: self.grade)
    }
}

/// This is to test legacy support. It is identical to the Homework class, but with a different class name and attribute names.
/// Homework has legacy keys added to its init(dataObject: DataObject) so if legacy support is implemented correctly, you should be able to
/// save an instance of LegacyHomework and restore it as Homework.
internal class LegacyHomework: Storable {
    // MARK: Nested Types

    private enum Field: String {
        case legacyAnswers
        case legacyGrade
    }

    // MARK: Properties

    internal let legacyAnswers: String

    internal private(set) var legacyGrade: Int?

    // MARK: Lifecycle

    internal init(legacyAnswers: String, legacyGrade: Int?) {
        self.legacyAnswers = legacyAnswers
        self.legacyGrade = legacyGrade
    }

    internal required init(dataObject: DataObject) {
        self.legacyAnswers = dataObject.get(Field.legacyAnswers.rawValue)
        self.legacyGrade = dataObject.get(Field.legacyGrade.rawValue)
    }

    // MARK: Functions

    internal func toDataObject() -> DataObject {
        return DataObject(self)
            .add(key: Field.legacyAnswers.rawValue, value: self.legacyAnswers)
            .add(key: Field.legacyGrade.rawValue, value: self.legacyGrade)
    }
}
