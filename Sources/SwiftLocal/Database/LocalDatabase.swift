//
//  LocalDatabase.swift
//  SwiftLocal
//
//  Created by Andre Pham on 5/1/2023.
//

import Foundation
import SQLite3

public enum LocalDatabaseError: Error {
    
    /// An error relating to opening the database
    case databaseOpenError(String)
    /// An error relating to preparing an SQLite3 statement
    case statementPreparationError(String)
    /// An error relating to an SQLite3 statement execution failing
    case executionError(String)
    /// An error relating to a transaction
    case transactionError(String)
    
}

/// A database target that uses the SQLite3 to save data.
public class LocalDatabase {
    
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
    /// A dedicated serial queue to serialize all SQLite access - allows database to be accessed by multiple concurrent threads
    private let databaseQueue = DispatchQueue(label: "swiftlocal.andrepham")
    
    public init() throws {
        self.url = FileManager.default
            .urls(for: .libraryDirectory, in: .allDomainsMask)[0]
            .appendingPathComponent("records.sqlite")
        guard sqlite3_open(self.url.path, &self.database) == SQLITE_OK else {
            throw LocalDatabaseError.databaseOpenError("SQLite database could not be opened at path: \(self.url.path)")
        }
        try self.setupTable()
    }
    
    deinit {
        if self.database != nil {
            sqlite3_close(self.database)
        }
    }
    
    private func setupTable() throws {
        let statementString = """
        CREATE TABLE IF NOT EXISTS record(
            id TEXT PRIMARY KEY,
            objectName TEXT,
            data TEXT
        );
        """
        var statement: OpaquePointer? = nil
        guard sqlite3_prepare_v2(self.database, statementString, -1, &statement, nil) == SQLITE_OK else {
            throw LocalDatabaseError.statementPreparationError("Failed to prepare table creation statement")
        }
        guard sqlite3_step(statement) == SQLITE_DONE else {
            sqlite3_finalize(statement)
            throw LocalDatabaseError.executionError("Failed to create table")
        }
        sqlite3_finalize(statement)
    }
    
    /// A centralized helper that runs work on the dedicated database queue.
    private func perform<T>(_ block: @escaping () throws -> T) async throws -> T {
        return try await withCheckedThrowingContinuation { continuation in
            self.databaseQueue.async {
                do {
                    let result = try block()
                    continuation.resume(returning: result)
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }
    
    /// Write a record to the database. If the id already exists, replace it.
    /// - Parameters:
    ///   - record: The record to be written
    public func write<T: Storable>(_ record: Record<T>) async throws {
        try await self.perform {
            let statementString = "REPLACE INTO record (id, objectName, data) VALUES (?, ?, ?);"
            var statement: OpaquePointer? = nil
            guard sqlite3_prepare_v2(self.database, statementString, -1, &statement, nil) == SQLITE_OK else {
                throw LocalDatabaseError.statementPreparationError("Failed to prepare write statement")
            }
            sqlite3_bind_text(statement, 1, (record.metadata.id as NSString).utf8String, -1, nil)
            sqlite3_bind_text(statement, 2, (record.metadata.objectName as NSString).utf8String, -1, nil)
            sqlite3_bind_text(statement, 3, (String(decoding: record.data.toDataObject().rawData, as: UTF8.self) as NSString).utf8String, -1, nil)
            let successful = sqlite3_step(statement) == SQLITE_DONE
            if self.transactionActive {
                sqlite3_reset(statement)
            } else {
                sqlite3_finalize(statement)
            }
            if !successful {
                throw LocalDatabaseError.executionError("Failed to write record for object: \(record.metadata.objectName)")
            }
        }
    }
    
    /// Retrieve all storable objects of a specified type.
    /// - Returns: All saved objects of the specified type
    public func read<T: Storable>() async throws -> [T] {
        return try await self.perform {
            let currentObjectName = String(describing: T.self)
            let legacyObjectNames = Legacy.oldClassNames[currentObjectName] ?? []
            let allObjectNames = legacyObjectNames + [currentObjectName]
            var result = [T]()
            for objectName in allObjectNames {
                let statementString = "SELECT * FROM record WHERE objectName = ?;"
                var statement: OpaquePointer? = nil
                guard sqlite3_prepare_v2(self.database, statementString, -1, &statement, nil) == SQLITE_OK else {
                    throw LocalDatabaseError.statementPreparationError("Failed to prepare read statement for object: \(objectName)")
                }
                sqlite3_bind_text(statement, 1, (objectName as NSString).utf8String, -1, nil)
                while sqlite3_step(statement) == SQLITE_ROW {
                    guard let dataCStr = sqlite3_column_text(statement, 3) else {
                        continue
                    }
                    let dataString = String(cString: dataCStr)
                    guard let data = dataString.data(using: .utf8) else {
                        continue
                    }
                    let dataObject = DataObject(data: data)
                    result.append(dataObject.restore(T.self))
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
    public func read<T: Storable>(id: String) async throws -> T? {
        return try await self.perform {
            let statementString = "SELECT * FROM record WHERE id = ?;"
            var statement: OpaquePointer? = nil
            guard sqlite3_prepare_v2(self.database, statementString, -1, &statement, nil) == SQLITE_OK else {
                throw LocalDatabaseError.statementPreparationError("Failed to prepare read statement for id: \(id)")
            }
            sqlite3_bind_text(statement, 1, (id as NSString).utf8String, -1, nil)
            var result: T? = nil
            if sqlite3_step(statement) == SQLITE_ROW {
                guard let dataCStr = sqlite3_column_text(statement, 3) else {
                    sqlite3_finalize(statement)
                    return nil
                }
                let dataString = String(cString: dataCStr)
                if let data = dataString.data(using: .utf8) {
                    let dataObject = DataObject(data: data)
                    result = dataObject.restore(T.self)
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
    public func readIDs<T: Storable>(_ allOf: T.Type) async throws -> [String] {
        return try await self.perform {
            let currentObjectName = String(describing: T.self)
            let legacyObjectNames = Legacy.oldClassNames[currentObjectName] ?? []
            let allObjectNames = legacyObjectNames + [currentObjectName]
            var result = [String]()
            for objectName in allObjectNames {
                let statementString = "SELECT id FROM record WHERE objectName = ?;"
                var statement: OpaquePointer? = nil
                guard sqlite3_prepare_v2(self.database, statementString, -1, &statement, nil) == SQLITE_OK else {
                    throw LocalDatabaseError.statementPreparationError("Failed to prepare readIDs statement for object: \(objectName)")
                }
                sqlite3_bind_text(statement, 1, (objectName as NSString).utf8String, -1, nil)
                while sqlite3_step(statement) == SQLITE_ROW {
                    if let idCStr = sqlite3_column_text(statement, 0) {
                        let id = String(cString: idCStr)
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
    public func delete<T: Storable>(_ allOf: T.Type) async throws -> Int {
        return try await self.perform {
            let countBeforeDelete = try self.countInternal()
            let currentObjectName = String(describing: T.self)
            let legacyObjectNames = Legacy.oldClassNames[currentObjectName] ?? []
            let allObjectNames = legacyObjectNames + [currentObjectName]
            for objectName in allObjectNames {
                let statementString = "DELETE FROM record WHERE objectName = ?;"
                var statement: OpaquePointer? = nil
                guard sqlite3_prepare_v2(self.database, statementString, -1, &statement, nil) == SQLITE_OK else {
                    throw LocalDatabaseError.statementPreparationError("Failed to prepare delete statement for object: \(objectName)")
                }
                sqlite3_bind_text(statement, 1, (objectName as NSString).utf8String, -1, nil)
                sqlite3_step(statement)
                if self.transactionActive {
                    sqlite3_reset(statement)
                } else {
                    sqlite3_finalize(statement)
                }
            }
            let countAfterDelete = try self.countInternal()
            return countBeforeDelete - countAfterDelete
        }
    }
    
    /// Delete the record with the matching id.
    /// - Parameters:
    ///   - id: The id of the stored record to delete
    public func delete(id: String) async throws {
        try await self.perform {
            let statementString = "DELETE FROM record WHERE id = ?;"
            var statement: OpaquePointer? = nil
            guard sqlite3_prepare_v2(self.database, statementString, -1, &statement, nil) == SQLITE_OK else {
                throw LocalDatabaseError.statementPreparationError("Failed to prepare delete statement for id: \(id)")
            }
            sqlite3_bind_text(statement, 1, (id as NSString).utf8String, -1, nil)
            let successful = sqlite3_step(statement) == SQLITE_DONE
            if self.transactionActive {
                sqlite3_reset(statement)
            } else {
                sqlite3_finalize(statement)
            }
            if !successful {
                throw LocalDatabaseError.executionError("Failed to delete record with id: \(id)")
            }
        }
    }
    
    /// Clear the entire database.
    /// - Returns: The number of records deleted
    @discardableResult
    public func clearDatabase() async throws -> Int {
        return try await self.perform {
            let count = try self.countInternal()
            var countDeleted = 0
            let statementString = "DELETE FROM record;"
            var statement: OpaquePointer? = nil
            guard sqlite3_prepare_v2(self.database, statementString, -1, &statement, nil) == SQLITE_OK else {
                throw LocalDatabaseError.statementPreparationError("Failed to prepare clearDatabase statement")
            }
            let successful = sqlite3_step(statement) == SQLITE_DONE
            if successful {
                // Only if successful can we can assign the previous count (before clearing the database) to our return value
                countDeleted = count
            }
            if self.transactionActive {
                sqlite3_reset(statement)
            } else {
                sqlite3_finalize(statement)
            }
            if !successful {
                throw LocalDatabaseError.executionError("Failed to clear database")
            }
            return countDeleted
        }
    }
    
    /// Count the number of records saved.
    /// - Returns: The number of records
    public func count() async throws -> Int {
        return try await self.perform {
            try self.countInternal()
        }
    }
    
    /// Count the number of records saved. Executed without queuing.
    /// WARNING: Does not operate using the database queue - only execute this within a database queue sync block.
    /// - Returns: The number of records
    private func countInternal() throws -> Int {
        let statementString = "SELECT COUNT(*) FROM record;"
        var statement: OpaquePointer? = nil
        guard sqlite3_prepare(self.database, statementString, -1, &statement, nil) == SQLITE_OK else {
            throw LocalDatabaseError.statementPreparationError("Failed to prepare count statement")
        }
        guard sqlite3_step(statement) == SQLITE_ROW else {
            sqlite3_finalize(statement)
            throw LocalDatabaseError.executionError("Counting records statement could not be executed")
        }
        let count = Int(sqlite3_column_int(statement, 0))
        sqlite3_finalize(statement)
        return count
    }
    
    /// Count the number of records of a certain type saved.
    /// - Parameters:
    ///   - allOf: The type to count
    /// - Returns: The number of records of the provided type currently saved
    public func count<T: Storable>(_ allOf: T.Type) async throws -> Int {
        return try await self.perform {
            var count = 0
            let currentObjectName = String(describing: T.self)
            let legacyObjectNames = Legacy.oldClassNames[currentObjectName] ?? []
            let allObjectNames = legacyObjectNames + [currentObjectName]
            for objectName in allObjectNames {
                let statementString = "SELECT COUNT(*) FROM record WHERE objectName = ?;"
                var statement: OpaquePointer? = nil
                guard sqlite3_prepare(self.database, statementString, -1, &statement, nil) == SQLITE_OK else {
                    throw LocalDatabaseError.statementPreparationError("Failed to prepare count statement for object: \(objectName)")
                }
                sqlite3_bind_text(statement, 1, (objectName as NSString).utf8String, -1, nil)
                if sqlite3_step(statement) == SQLITE_ROW {
                    count += Int(sqlite3_column_int(statement, 0))
                } else {
                    sqlite3_finalize(statement)
                    throw LocalDatabaseError.executionError("Counting records statement could not be executed for object: \(objectName)")
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
    public func startTransaction(override: Bool = true) async throws {
        try await self.perform {
            if self.transactionActive {
                if !override {
                    throw LocalDatabaseError.transactionError("Transaction already active and override is false")
                }
                try self.rollbackTransactionInternal()
            }
            let beginTransactionString = "BEGIN TRANSACTION;"
            var statement: OpaquePointer? = nil
            guard sqlite3_prepare_v2(self.database, beginTransactionString, -1, &statement, nil) == SQLITE_OK else {
                throw LocalDatabaseError.statementPreparationError("Failed to prepare startTransaction statement")
            }
            guard sqlite3_step(statement) == SQLITE_DONE else {
                sqlite3_finalize(statement)
                throw LocalDatabaseError.executionError("Failed to start transaction")
            }
            sqlite3_finalize(statement)
            self.transactionActive = true
        }
    }
    
    /// Commit the current transaction. All changes made during the transaction are finalised.
    public func commitTransaction() async throws {
        try await self.perform {
            guard self.transactionActive else {
                throw LocalDatabaseError.transactionError("No active transaction to commit")
            }
            let commitTransactionString = "COMMIT;"
            var statement: OpaquePointer? = nil
            guard sqlite3_prepare_v2(self.database, commitTransactionString, -1, &statement, nil) == SQLITE_OK else {
                throw LocalDatabaseError.statementPreparationError("Failed to prepare commitTransaction statement")
            }
            guard sqlite3_step(statement) == SQLITE_DONE else {
                sqlite3_finalize(statement)
                throw LocalDatabaseError.executionError("Failed to commit transaction")
            }
            sqlite3_finalize(statement)
            self.transactionActive = false
        }
    }
    
    /// Rollback the current transaction. All changes made during the transaction are undone.
    /// - Returns: True if there was an active transaction and it was rolled back
    public func rollbackTransaction() async throws {
        try await self.perform {
            try self.rollbackTransactionInternal()
        }
    }
    
    /// Rollback the current transaction. All changes made during the transaction are undone. Executed without queuing.
    /// WARNING: Does not operate using the database queue - only execute this within a database queue sync block.
    private func rollbackTransactionInternal() throws {
        guard self.transactionActive else {
            throw LocalDatabaseError.transactionError("No active transaction to rollback")
        }
        let rollbackTransactionString = "ROLLBACK;"
        var statement: OpaquePointer? = nil
        guard sqlite3_prepare_v2(self.database, rollbackTransactionString, -1, &statement, nil) == SQLITE_OK else {
            throw LocalDatabaseError.statementPreparationError("Failed to prepare rollbackTransaction statement")
        }
        guard sqlite3_step(statement) == SQLITE_DONE else {
            sqlite3_finalize(statement)
            throw LocalDatabaseError.executionError("Failed to rollback transaction")
        }
        sqlite3_finalize(statement)
        self.transactionActive = false
    }
    
}
