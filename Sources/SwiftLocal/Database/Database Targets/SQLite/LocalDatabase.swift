//
//  LocalDatabase.swift
//  SwiftLocal
//
//  Created by Andre Pham on 5/1/2023.
//

import Foundation
import SQLite3

/// A database target that uses the SQLite3 to save data.
public class LocalDatabase: DatabaseTarget {
    
    /// The directory the sqlite file is saved to
    private let url: URL
    /// The database instance
    private var database: OpaquePointer? = nil
    /// The date formatter used for adding and retrieving dates
    private var dateFormatter: DateFormatter {
        let result = DateFormatter()
        result.locale = Locale(identifier: "en_US_POSIX")
        result.timeZone = TimeZone(secondsFromGMT: 0)
        result.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZZ"
        return result
    }
    /// True if a transaction is ongoing
    public private(set) var transactionActive = false
    /// The queue of tasks for the database to complete - allows database to be accessed by multiple concurrent threads
    /// It ensures every database access is serialized so only one operation can access the database at a time
    /// Otherwise database access from multiple concurrent threads can cause the error "illegal multi-threaded access to database connection"
    /// Tip: To ensure no deadlocks, make sure a task added to the queue doesn't start another task
    /// Tip: If accessing the database is required before starting another operation, both database accesses should be completed within a single sync block, otherwise they can become out of sync (this is accomplished using "internal" denoted methods that execute without being queued so they can be called within sync blocks)
    private let databaseQueue = DispatchQueue(label: "swiftlocal.andrepham")
    
    public init() {
        self.url = FileManager.default.urls(for: .libraryDirectory, in: .allDomainsMask)[0]
            .appendingPathComponent("records.sqlite")
        guard sqlite3_open(self.url.path, &self.database) == SQLITE_OK else {
            fatalError("SQLite database could not be opened")
        }
        self.setupTable()
    }
    
    deinit {
        if self.database != nil {
            sqlite3_close(self.database)
        }
    }
    
    private func setupTable() {
        let statementString = """
        CREATE TABLE IF NOT EXISTS record(
            id TEXT PRIMARY KEY,
            objectName TEXT,
            createdAt TEXT,
            data TEXT
        );
        """
        var statement: OpaquePointer? = nil
        if sqlite3_prepare_v2(self.database, statementString, -1, &statement, nil) == SQLITE_OK {
            let outcome = sqlite3_step(statement) == SQLITE_DONE
            assert(outcome, "SQLite table could not be created")
            sqlite3_finalize(statement)
        }
    }
    
    /// Write a record to the database. If the id already exists, replace it.
    /// - Parameters:
    ///   - record: The record to be written
    /// - Returns: If the write was successful
    @discardableResult
    public func write<T: Storable>(_ record: Record<T>) -> Bool {
        return self.databaseQueue.sync {
            let statementString = "REPLACE INTO record (id, objectName, createdAt, data) VALUES (?, ?, ?, ?);"
            var statement: OpaquePointer? = nil
            guard sqlite3_prepare_v2(self.database, statementString, -1, &statement, nil) == SQLITE_OK else {
                return false
            }
            sqlite3_bind_text(statement, 1, (record.metadata.id as NSString).utf8String, -1, nil)
            sqlite3_bind_text(statement, 2, (record.metadata.objectName as NSString).utf8String, -1, nil)
            sqlite3_bind_text(statement, 3, (self.dateFormatter.string(from: record.metadata.createdAt) as NSString).utf8String, -1, nil)
            sqlite3_bind_text(statement, 4, (String(decoding: record.data.toDataObject().rawData, as: UTF8.self) as NSString).utf8String, -1, nil)
            let outcome = sqlite3_step(statement) == SQLITE_DONE
            if self.transactionActive {
                sqlite3_reset(statement)
            } else {
                sqlite3_finalize(statement)
            }
            return outcome
        }
    }
    
    /// Retrieve all storable objects of a specified type.
    /// - Returns: All saved objects of the specified type
    public func read<T: Storable>() -> [T] {
        return self.databaseQueue.sync {
            let currentObjectName = String(describing: T.self)
            let legacyObjectNames = Legacy.oldClassNames[currentObjectName]
            let allObjectNames = (legacyObjectNames ?? [String]()) + [currentObjectName]
            var result = [T]()
            for objectName in allObjectNames {
                let statementString = "SELECT * FROM record WHERE objectName = ?;"
                var statement: OpaquePointer? = nil
                if sqlite3_prepare_v2(self.database, statementString, -1, &statement, nil) == SQLITE_OK {
                    sqlite3_bind_text(statement, 1, (objectName as NSString).utf8String, -1, nil)
                    while sqlite3_step(statement) == SQLITE_ROW {
                        // These may come in handy later:
                        //let id = String(describing: String(cString: sqlite3_column_text(statement, 0)))
                        //let objectName = String(describing: String(cString: sqlite3_column_text(statement, 1)))
                        //let createdAt = self.dateFormatter.date(from: String(describing: String(cString: sqlite3_column_text(statement, 2)))) ?? Date()
                        let dataString = String(describing: String(cString: sqlite3_column_text(statement, 3)))
                        guard let data = dataString.data(using: .utf8) else {
                            continue
                        }
                        let dataObject = DataObject(data: data)
                        result.append(dataObject.restore(T.self))
                    }
                }
                sqlite3_finalize(statement)
            }
            return result
        }
    }
    
    /// Retrieve the storable object with the matching id.
    /// - Parameters:
    ///   - id: The id of the stored record
    /// - Returns: The storable object with the matching id
    public func read<T: Storable>(id: String) -> T? {
        return self.databaseQueue.sync {
            let statementString = "SELECT * FROM record WHERE id = ?;"
            var statement: OpaquePointer? = nil
            var result: T? = nil
            if sqlite3_prepare_v2(self.database, statementString, -1, &statement, nil) == SQLITE_OK {
                sqlite3_bind_text(statement, 1, (id as NSString).utf8String, -1, nil)
                if sqlite3_step(statement) == SQLITE_ROW {
                    let dataString = String(describing: String(cString: sqlite3_column_text(statement, 3)))
                    if let data = dataString.data(using: .utf8) {
                        let dataObject = DataObject(data: data)
                        result = dataObject.restore(T.self)
                    }
                }
            }
            sqlite3_finalize(statement)
            return result
        }
    }
    
    /// Retrieve all the record IDs of all objects of a specific type.
    /// - Parameters:
    ///   - allOf: The type to retrieve the ids from
    /// - Returns: All stored record ids of the provided type
    public func readIDs<T: Storable>(_ allOf: T.Type) -> [String] {
        return self.databaseQueue.sync {
            let currentObjectName = String(describing: T.self)
            let legacyObjectNames = Legacy.oldClassNames[currentObjectName]
            let allObjectNames = (legacyObjectNames ?? [String]()) + [currentObjectName]
            var result = [String]()
            for objectName in allObjectNames {
                let statementString = "SELECT id FROM record WHERE objectName = ?;"
                var statement: OpaquePointer? = nil
                if sqlite3_prepare_v2(self.database, statementString, -1, &statement, nil) == SQLITE_OK {
                    sqlite3_bind_text(statement, 1, (objectName as NSString).utf8String, -1, nil)
                    while sqlite3_step(statement) == SQLITE_ROW {
                        let id = String(describing: String(cString: sqlite3_column_text(statement, 0)))
                        result.append(id)
                    }
                }
                sqlite3_finalize(statement)
            }
            return result
        }
    }
    
    /// Delete all instances of an object
    /// - Parameters:
    ///   - allOf: The type to delete
    /// - Returns: The number of records deleted
    @discardableResult
    public func delete<T: Storable>(_ allOf: T.Type) -> Int {
        return self.databaseQueue.sync {
            let countBeforeDelete = self.countInternal()
            let currentObjectName = String(describing: T.self)
            let legacyObjectNames = Legacy.oldClassNames[currentObjectName]
            let allObjectNames = (legacyObjectNames ?? [String]()) + [currentObjectName]
            for objectName in allObjectNames {
                let statementString = "DELETE FROM record WHERE objectName = ?;"
                var statement: OpaquePointer? = nil
                if sqlite3_prepare_v2(self.database, statementString, -1, &statement, nil) == SQLITE_OK {
                    sqlite3_bind_text(statement, 1, (objectName as NSString).utf8String, -1, nil)
                    sqlite3_step(statement)
                }
                if self.transactionActive {
                    sqlite3_reset(statement)
                } else {
                    sqlite3_finalize(statement)
                }
            }
            return countBeforeDelete - self.countInternal()
        }
    }
    
    /// Delete the record with the matching id.
    /// - Parameters:
    ///   - id: The id of the stored record to delete
    /// - Returns: If any record was successfully deleted
    @discardableResult
    public func delete(id: String) -> Bool {
        return self.databaseQueue.sync {
            var successful = false
            let statementString = "DELETE FROM record WHERE id = ?;"
            var statement: OpaquePointer? = nil
            if sqlite3_prepare_v2(self.database, statementString, -1, &statement, nil) == SQLITE_OK {
                sqlite3_bind_text(statement, 1, (id as NSString).utf8String, -1, nil)
                successful = sqlite3_step(statement) == SQLITE_DONE
            }
            if self.transactionActive {
                sqlite3_reset(statement)
            } else {
                sqlite3_finalize(statement)
            }
            return successful
        }
    }
    
    /// Clear the entire database.
    /// - Returns: The number of records deleted
    @discardableResult
    public func clearDatabase() -> Int {
        return self.databaseQueue.sync {
            let count = self.countInternal()
            var countDeleted = 0
            let statementString = "DELETE FROM record;"
            var statement: OpaquePointer? = nil
            if sqlite3_prepare_v2(self.database, statementString, -1, &statement, nil) == SQLITE_OK {
                if sqlite3_step(statement) == SQLITE_DONE {
                    // Only if successful can we can assign the previous count (before clearing the database) to our return value
                    countDeleted = count
                }
            }
            if self.transactionActive {
                sqlite3_reset(statement)
            } else {
                sqlite3_finalize(statement)
            }
            return countDeleted
        }
    }
    
    /// Count the number of records saved.
    /// - Returns: The number of records
    public func count() -> Int {
        return self.databaseQueue.sync {
            return self.countInternal()
        }
    }
    
    /// Count the number of records saved. Executed without queuing.
    /// WARNING: Does not operate using the database queue - only execute this within a database queue sync block.
    /// - Returns: The number of records
    private func countInternal() -> Int {
        var count = 0
        let statementString = "SELECT COUNT(*) FROM record;"
        var statement: OpaquePointer? = nil
        if sqlite3_prepare(self.database, statementString, -1, &statement, nil) == SQLITE_OK {
            if sqlite3_step(statement) == SQLITE_ROW {
                count = Int(sqlite3_column_int(statement, 0))
            } else {
                assertionFailure("Counting records statement could not be executed")
            }
        }
        sqlite3_finalize(statement)
        return count
    }
    
    /// Count the number of records of a certain type saved.
    /// - Parameters:
    ///   - allOf: The type to count
    /// - Returns: The number of records of the provided type currently saved
    public func count<T: Storable>(_ allOf: T.Type) -> Int {
        return self.databaseQueue.sync {
            var count = 0
            let currentObjectName = String(describing: T.self)
            let legacyObjectNames = Legacy.oldClassNames[currentObjectName]
            let allObjectNames = (legacyObjectNames ?? [String]()) + [currentObjectName]
            for objectName in allObjectNames {
                let statementString = "SELECT COUNT(*) FROM record WHERE objectName = ?;"
                var statement: OpaquePointer? = nil
                if sqlite3_prepare(self.database, statementString, -1, &statement, nil) == SQLITE_OK {
                    sqlite3_bind_text(statement, 1, (objectName as NSString).utf8String, -1, nil)
                    if sqlite3_step(statement) == SQLITE_ROW {
                        count += Int(sqlite3_column_int(statement, 0))
                    } else {
                        assertionFailure("Counting records statement could not be executed")
                    }
                }
                sqlite3_finalize(statement)
            }
            return count
        }
    }
    
    /// Begin a database transaction.
    /// Changes are still made immediately, however to finalise the transaction, `commitTransaction` should be executed.
    /// All changes made during the transaction are cancelled if `rollbackTransaction` is executed.
    /// If a new transaction is started before this one is committed, this transaction's changes are rolled back.
    /// - Parameters:
    ///   - override: Override (roll back) the current transaction if one is currently active already - true by default
    /// - Returns: True if the transaction was successfully started
    public func startTransaction(override: Bool = true) -> Bool {
        return self.databaseQueue.sync {
            if self.transactionActive {
                if !override {
                    // There already exists a transaction, and we can't override it!
                    return false
                }
                let rollbackSuccessful = self.rollbackTransactionInternal()
                if !rollbackSuccessful {
                    return false
                }
            }
            var result = false
            let beginTransactionString = "BEGIN TRANSACTION;"
            var statement: OpaquePointer? = nil
            if sqlite3_prepare_v2(self.database, beginTransactionString, -1, &statement, nil) == SQLITE_OK {
                result = sqlite3_step(statement) == SQLITE_DONE
                sqlite3_finalize(statement)
            }
            self.transactionActive = result
            return result
        }
    }
    
    /// Commit the current transaction. All changes made during the transaction are finalised.
    /// - Returns: True if there was an active transaction and it was committed
    public func commitTransaction() -> Bool {
        return self.databaseQueue.sync {
            guard self.transactionActive else {
                return false
            }
            var result = false
            let commitTransactionString = "COMMIT;"
            var statement: OpaquePointer? = nil
            if sqlite3_prepare_v2(self.database, commitTransactionString, -1, &statement, nil) == SQLITE_OK {
                result = sqlite3_step(statement) == SQLITE_DONE
                sqlite3_finalize(statement)
            }
            self.transactionActive = self.transactionActive ? !result : false
            return result
        }
    }
    
    /// Rollback the current transaction. All changes made during the transaction are undone.
    /// - Returns: True if there was an active transaction and it was rolled back
    public func rollbackTransaction() -> Bool {
        return self.databaseQueue.sync {
            return self.rollbackTransactionInternal()
        }
    }
    
    /// Rollback the current transaction. All changes made during the transaction are undone. Executed without queuing.
    /// WARNING: Does not operate using the database queue - only execute this within a database queue sync block.
    /// - Returns: True if there was an active transaction and it was rolled back
    private func rollbackTransactionInternal() -> Bool {
        guard self.transactionActive else {
            return false
        }
        var result = false
        let rollbackTransactionString = "ROLLBACK;"
        var statement: OpaquePointer? = nil
        if sqlite3_prepare_v2(self.database, rollbackTransactionString, -1, &statement, nil) == SQLITE_OK {
            result = sqlite3_step(statement) == SQLITE_DONE
            sqlite3_finalize(statement)
        }
        self.transactionActive = self.transactionActive ? !result : false
        return result
    }
    
}
