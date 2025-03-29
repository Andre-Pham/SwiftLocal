//
//  DatabaseTarget.swift
//  SwiftSerialization
//
//  Created by Andre Pham on 3/1/2023.
//

import Foundation

public protocol DatabaseTarget {
    
    /// True if a transaction is ongoing
    var transactionActive: Bool { get }
    
    /// Write a record to the database. If the id already exists, replace it.
    /// - Parameters:
    ///   - record: The record to be written
    /// - Returns: If the write was successful
    func write<T: Storable>(_ record: Record<T>) -> Bool
    
    /// Retrieve all storable objects of a specified type.
    /// - Returns: All saved objects of the specified type
    func read<T: Storable>() -> [T]
    
    /// Retrieve the storable object with the matching id.
    /// - Parameters:
    ///   - id: The id of the stored record
    /// - Returns: The storable object with the matching id
    func read<T: Storable>(id: String) -> T?
    
    /// Retrieve all the record IDs of all objects of a specific type.
    /// - Parameters:
    ///   - allOf: The type to retrieve the ids from
    /// - Returns: All stored record ids of the provided type
    func readIDs<T: Storable>(_ allOf: T.Type) -> [String]
    
    /// Delete all instances of an object
    /// - Parameters:
    ///   - allOf: The type to delete
    /// - Returns: The number of records deleted
    func delete<T: Storable>(_ allOf: T.Type) -> Int
    
    /// Delete the record with the matching id.
    /// - Parameters:
    ///   - id: The id of the stored record to delete
    /// - Returns: If any record was successfully deleted
    func delete(id: String) -> Bool
    
    /// Clear the entire database.
    /// - Returns: The number of records deleted
    func clearDatabase() -> Int
    
    /// Count the number of records saved.
    /// - Returns: The number of records
    func count() -> Int
    
    /// Count the number of records of a certain type saved.
    /// - Parameters:
    ///   - allOf: The type to count
    /// - Returns: The number of records of the provided type currently saved
    func count<T: Storable>(_ allOf: T.Type) -> Int
    
    /// Begin a database transaction.
    /// Changes are still made immediately, however to finalise the transaction, `commitTransaction` should be executed.
    /// All changes made during the transaction are cancelled if `rollbackTransaction` is executed.
    /// If a new transaction is started before this one is committed, this transaction's changes are rolled back.
    /// - Parameters:
    ///   - override: Override (roll back) the current transaction if one is currently active already - true by default
    /// - Returns: True if the transaction was successfully started
    func startTransaction(override: Bool) -> Bool
    
    /// Commit the current transaction. All changes made during the transaction are finalised.
    /// - Returns: True if there was an active transaction and it was committed
    func commitTransaction() -> Bool
    
    /// Rollback the current transaction. All changes made during the transaction are undone.
    /// - Returns: True if there was an active transaction and it was rolled back
    func rollbackTransaction() -> Bool
    
}
