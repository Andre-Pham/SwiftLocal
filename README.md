# SwiftLocal
A Swift package for locally serializing, persisting, and restoring Swift objects.

## Usage Examples

Any object that conforms to `Storable` can easily be written to and read from the database.

```swift
// Lets define a database first
// Realistically this would be managed at the application level
let database = try LocalDatabase()

// Here's the object we want to read/write
let person = Person(name: "Andre", height: 188.0)
```

Writing to the database. Returns `true` on success and `false` on failure.

```swift
// Writes our person object to the database
// Generates a random record ID
try await database.write(Record(data: person))

// You can also provide your own record ID
// (Overrides any record with the same ID)
try await database.write(Record(id: "myID", data: person))
```

There's three ways to read from the database.

```swift
// Reads a specific Person object by their record ID
let readPerson: Person? = try await database.read(id: "myID")

// Reads all saved Person objects
let readPeople: [Person] = try await database.read()

// Reads all saved Person record IDs
let readPeopleIDs: [String] = try await database.readIDs(Person.self)
```

We can count records. If we wish we can specify a specific object type.

```swift
// Counts all records in the database
let count = try await database.count()

// Counts the number of People records
let peopleCount = try await database.count(Person.self)
```

We can delete records, or even clear the database.

```swift
// Delete a record with specific record ID
// (Returns true if a record was deleted)
try await database.delete(id: "myID")

// Delete all Person records
// (Returns the number of records deleted)
try await database.delete(Person.self)

// Delete everything
// (Returns the number of records deleted)
try await database.clearDatabase()
```

We can also use transactions if we wish. So long as we haven't committed yet, all changes made during a transaction can be rolled back.

```swift
try await database.startTransaction()
let person1Saved = try await database.write(Record(data: person1))
let person2Saved = try await database.write(Record(data: person2))
if person1Saved && person2Saved {
    // We can commit the transaction to finalise the changes
    try await database.commitTransaction()
} else {
    // Or we can rollback to undo all changes made during the transaction
    try await database.rollbackTransaction()
}
```

Everything works asynchronously with Swift Concurrency, with the database built to handle multiple concurrent threads.

```swift
Task {
    do {
        let readPerson: Person? = try await database.read(id: "myID")
        await MainActor.run {
            // Update UI
        }
    } catch {
        print(error)
    }
}
```

## Storable Class Examples

Class definitions, and how you would conform them to `Storable` to be serialized.

```swift
class Person: Storable {
    
    private(set) var firstName: String
    private(set) var lastName: String
    
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
        // NOTE: You can also override the default return value if the value can't be found
        // self.lastName = dataObject.get(Field.lastName.rawValue, onFail: "MISSING")
    }
    
    func toDataObject() -> DataObject {
        return DataObject(self)
            .add(key: Field.firstName.rawValue, value: self.firstName)
            .add(key: Field.lastName.rawValue, value: self.lastName)
    }
    
}

class Student: Person {
    
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

class Teacher: Person {
    
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

class Homework: Storable {
    
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
        self.answers = dataObject.get(Field.answers.rawValue)
        self.grade = dataObject.get(Field.grade.rawValue)
    }
    
    func toDataObject() -> DataObject {
        return DataObject(self)
            .add(key: Field.answers.rawValue, value: self.answers)
            .add(key: Field.grade.rawValue, value: self.grade)
    }
    
}
```

Serialization works with `typealias` definitions, but the syntax is a little different.

```swift
class Person // ...
protocol HasDebt // ...

typealias Student = Person & HasDebt

// ...

// In init(dataObject: DataObject)
self.students = dataObject.getObjectArray(Field.students.rawValue, type: Person.self) as! [any Student]

// In toDataObject() -> DataObject
.add(key: Field.students.rawValue, value: self.students as [Person])
```

## Handling Property Addition/Removal

Your classes will change over time.

If you remove properties from your class but had saved it previously, just don't read it from the `DataObject`.

If you had previously saved your objects then later added new properties to their class definition, you define within the class initialiser how the class handles the missing data.

```swift
private var firstName: String

// ...

// By default, self.firstName will be set to "" if no value is returned
self.firstName = dataObject.get(Field.firstName.rawValue)

// self.firstName will be set to "MISSING" if no value is returned
self.firstName = dataObject.get(Field.firstName.rawValue, onFail: "MISSING")
```

If the property is optional and no value is returned, it will be set to `nil`.

```swift
private var firstName: String?

// ...

// self.firstName will be set to nil if no value is returned
self.firstName = dataObject.get(Field.firstName.rawValue)
```

## Handling Refactoring

You may change the names of your classes and properties. You have to account for these refactors.

Here's how your class may look beforehand:

```swift
class Person: Storable {
    
    private(set) var name: String
    
    init(name: String) {
        self.name = name
    }
    
    // MARK: - Serialization
    
    private enum Field: String {
        case name
    }
    
    required init(dataObject: DataObject) {
        self.name = dataObject.get(Field.name.rawValue)
    }
    
    func toDataObject() -> DataObject {
        return DataObject(self)
            .add(key: Field.name.rawValue, value: self.name)
    }
    
}
```

The rules to follow are basically:

* For class name refactors, call `Legacy.addClassRefactor` with the new name.
* For property name refactors, assuming you change the `Field` case, include the `legacyKeys` parameter in the `.get` method call for that property.

If you refactor `Person` to `Human`, and `name` to `firstName`, the result should look as such:

```swift
// On application startup
Legacy.addClassRefactor(old: "Person", new: "Human")

// ...

class Human: Storable {
    
    private(set) var firstName: String
    
    init(firstName: String) {
        self.firstName = firstName
    }
    
    // MARK: - Serialization
    
    private enum Field: String {
        case firstName
    }
    
    required init(dataObject: DataObject) {
        self.firstName = dataObject.get(Field.firstName.rawValue, legacyKeys: ["name"])
    }
    
    func toDataObject() -> DataObject {
        return DataObject(self)
            .add(key: Field.firstName.rawValue, value: self.firstName)
    }
    
}
```

## `LocalDatabase` Documentation

#### Initializers

```swift
/// Initialize a new LocalDatabase instance.
/// - Throws: If the database could not be opened, or if the table could not be created
init() throws
```

#### Instance Properties

```swift
/// True if a transaction is ongoing
var transactionActive: Bool
```

#### Instance Methods

```swift
/// Write a record to the database. If the id already exists, replace it.
/// - Parameters:
///   - record: The record to be written
/// - Throws: If the write operation fails
func write<T: Storable>(_ record: Record<T>) async throws
```

```swift
/// Retrieve all storable objects of a specified type.
/// - Returns: All saved objects of the specified type
/// - Throws: If the read operation fails
func read<T: Storable>() async throws -> [T]
```

```swift
/// Retrieve the storable object with the matching id.
/// - Parameters:
///   - id: The id of the stored record
/// - Returns: The storable object with the matching id (nil if not found)
/// - Throws: If the read operation fails
func read<T: Storable>(id: String) async throws -> T?
```

```swift
/// Retrieve all the record IDs of all objects of a specific type.
/// - Parameters:
///   - allOf: The type to retrieve the ids from
/// - Returns: All stored record ids of the provided type
/// - Throws: If the read operation fails
func readIDs<T: Storable>(_ allOf: T.Type) async throws -> [String]
```

```swift
/// Delete all instances of an object.
/// - Parameters:
///   - allOf: The type to delete
/// - Returns: The number of records deleted
/// - Throws: If the delete operation fails
func delete<T: Storable>(_ allOf: T.Type) async throws -> Int
```

```swift
/// Delete the record with the matching id.
/// - Parameters:
///   - id: The id of the stored record to delete
/// - Throws: If the delete operation fails
func delete(id: String) async throws
```

```swift
/// Clear the entire database.
/// - Returns: The number of records deleted
/// - Throws: If the delete operation fails
func clearDatabase() async throws -> Int
```

```swift
/// Count the number of records saved.
/// - Returns: The number of records
/// - Throws: If the count operation fails
func count() async throws -> Int
```

```swift
/// Count the number of records of a certain type saved.
/// - Parameters:
///   - allOf: The type to count
/// - Returns: The number of records of the provided type currently saved
/// - Throws: If the count operation fails
func count<T: Storable>(_ allOf: T.Type) async throws -> Int
```

```swift
/// Begin a database transaction.
/// Changes are still made immediately, however to finalise the transaction, `commitTransaction` should be executed.
/// All changes made during the transaction are cancelled if `rollbackTransaction` is executed.
/// If a new transaction is started before this one is committed, this transaction's changes are rolled back.
/// - Parameters:
///   - override: Override (roll back) the current transaction if one is currently active already - true by default
/// - Throws: If a transaction is already active and `override` is false, or if the transaction operation fails
func startTransaction(override: Bool = true) async throws
```

```swift
/// Commit the current transaction. All changes made during the transaction are finalised.
/// - Throws: If no transaction is active, or if the commit operation fails
func commitTransaction() async throws
```

```swift
/// Rollback the current transaction. All changes made during the transaction are undone.
/// - Returns: True if there was an active transaction and it was rolled back
/// - Throws: If no transaction is active, or if the rollback operation fails
func rollbackTransaction() async throws
```

## `DataObject` Documentation

#### Initializers

```swift
/// Constructor.
/// - Parameters:
///   - object: The Storable object this will represent
init(_ object: Storable)
```

```swift
/// Constructor.
/// - Parameters:
///   - rawString: The raw JSON string to populate this with, generated from another DataObject
init(rawString: String)
```

#### Instance Properties

```swift
/// This DataObject's raw data
var rawData: Data
```

#### Instance Methods

```swift
func add(key: String, value: String) -> Self
```

```swift
func add(key: String, value: String?) -> Self
```

```swift
func add(key: String, value: [String]) -> Self
```

```swift
func add(key: String, value: [String?]) -> Self
```

```swift
func add(key: String, value: Int) -> Self
```

```swift
func add(key: String, value: Int?) -> Self
```

```swift
func add(key: String, value: [Int]) -> Self
```

```swift
func add(key: String, value: [Int?]) -> Self
```

```swift
func add(key: String, value: Double) -> Self
```

```swift
func add(key: String, value: Double?) -> Self
```

```swift
func add(key: String, value: [Double]) -> Self
```

```swift
func add(key: String, value: [Double?]) -> Self
```

```swift
func add(key: String, value: Bool) -> Self
```

```swift
func add(key: String, value: Bool?) -> Self
```

```swift
func add(key: String, value: [Bool]) -> Self
```

```swift
func add(key: String, value: [Bool?]) -> Self
```

```swift
func add(key: String, value: Date) -> Self
```

```swift
func add(key: String, value: Date?) -> Self
```

```swift
func add(key: String, value: [Date]) -> Self
```

```swift
func add(key: String, value: [Date?]) -> Self
```

```swift
func add<T: Storable>(key: String, value: T) -> Self
```

```swift
func add<T: Storable>(key: String, value: T?) -> Self
```

```swift
func add<T: Storable>(key: String, value: [T]) -> Self
```

```swift
func add<T: Storable>(key: String, value: [T?]) -> Self
```

```swift
func get(_ key: String, onFail: String = "", legacyKeys: [String] = []) -> String
```

```swift
func get(_ key: String, legacyKeys: [String] = []) -> String?
```

```swift
func get(_ key: String, legacyKeys: [String] = []) -> [String]
```

```swift
func get(_ key: String, legacyKeys: [String] = []) -> [String?]
```

```swift
func get(_ key: String, onFail: Int = 0, legacyKeys: [String] = []) -> Int
```

```swift
func get(_ key: String, legacyKeys: [String] = []) -> Int?
```

```swift
func get(_ key: String, legacyKeys: [String] = []) -> [Int]
```

```swift
func get(_ key: String, legacyKeys: [String] = []) -> [Int?]
```

```swift
func get(_ key: String, onFail: Double = 0.0, legacyKeys: [String] = []) -> Double
```

```swift
func get(_ key: String, legacyKeys: [String] = []) -> Double?
```

```swift
func get(_ key: String, legacyKeys: [String] = []) -> [Double]
```

```swift
func get(_ key: String, legacyKeys: [String] = []) -> [Double?]
```

```swift
func get(_ key: String, onFail: Bool, legacyKeys: [String] = []) -> Bool
```

```swift
func get(_ key: String, legacyKeys: [String] = []) -> Bool?
```

```swift
func get(_ key: String, legacyKeys: [String] = []) -> [Bool]
```

```swift
func get(_ key: String, legacyKeys: [String] = []) -> [Bool?]
```

```swift
func get(_ key: String, onFail: Date = Date(), legacyKeys: [String] = []) -> Date
```

```swift
func get(_ key: String, legacyKeys: [String] = []) -> Date?
```

```swift
func get(_ key: String, legacyKeys: [String] = []) -> [Date]
```

```swift
func get(_ key: String, legacyKeys: [String] = []) -> [Date?]
```

```swift
func getObject<T>(_ key: String, type: T.Type, legacyKeys: [String] = []) -> T where T: Storable
```

```swift
func getObjectOptional<T>(_ key: String, type: T.Type, legacyKeys: [String] = []) -> T? where T: Storable
```

```swift
func getObjectArray<T>(_ key: String, type: T.Type, legacyKeys: [String] = []) -> [T] where T: Storable
```

```swift
func toRawString() -> String?
```
