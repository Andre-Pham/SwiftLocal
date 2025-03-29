//
//  DataObject.swift
//  SwiftSerialization
//
//  Created by Andre Pham on 6/11/2022.
//

import Foundation
import SwiftyJSON
import SwiftUI

/// Refer to README.md for usage examples.
public class DataObject {
    
    // MARK: - Properties
    
    /// The JSON key that corresponds to the object name of the data object
    private let objectField = "object"
    /// The name of the object (class) this instance represents (the value to objectField)
    private(set) var objectName = String()
    /// The JSON this wrapper represents
    private var json = JSON()
    /// This DataObject's raw data
    public var rawData: Data {
        do {
            return try self.json.rawData()
        } catch {
            return Data()
        }
    }
    /// The date formatter used for adding and retrieving dates
    private var dateFormatter: DateFormatter {
        let result = DateFormatter()
        result.locale = Locale(identifier: "en_US_POSIX")
        result.timeZone = TimeZone(secondsFromGMT: 0)
        result.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZZ"
        return result
    }
    
    // MARK: - Initialisers
    
    /// Constructor.
    /// - Parameters:
    ///   - object: The Storable object this will represent
    public init(_ object: Storable) {
        self.add(key: self.objectField, value: object.className)
        self.objectName = object.className
    }
    
    /// Constructor.
    /// - Parameters:
    ///   - rawString: The raw JSON string to populate this with, generated from another DataObject
    public init(rawString: String) {
        self.json = JSON(parseJSON: rawString)
        self.objectName = self.get(self.objectField)
    }
    
    internal init(data: Data) {
        do {
            try self.json = JSON(data: data)
        } catch {
            print("JSON data could not be instantiated into a DataObject")
        }
        self.objectName = self.get(self.objectField)
    }
    
    private init(json: JSON) {
        self.json = json
        self.objectName = self.get(self.objectField)
    }
    
    // MARK: - Data addition methods
    // While generics can be used here, being explicit with data types allows compiler checking before runtime
    // Generics allows anything to be valid, causing crashes at runtime
    
    @discardableResult
    public func add(key: String, value: String) -> Self {
        self.json[key] = JSON(value)
        return self
    }
    
    @discardableResult
    public func add(key: String, value: String?) -> Self {
        self.json[key] = JSON(value ?? JSON.null)
        return self
    }
    
    @discardableResult
    public func add(key: String, value: [String]) -> Self {
        self.json[key] = JSON(value)
        return self
    }
    
    @discardableResult
    public func add(key: String, value: [String?]) -> Self {
        self.json[key] = JSON(value)
        return self
    }
    
    @discardableResult
    public func add(key: String, value: Int) -> Self {
        self.json[key] = JSON(value)
        return self
    }
    
    @discardableResult
    public func add(key: String, value: Int?) -> Self {
        self.json[key] = JSON(value ?? JSON.null)
        return self
    }
    
    @discardableResult
    public func add(key: String, value: [Int]) -> Self {
        self.json[key] = JSON(value)
        return self
    }
    
    @discardableResult
    public func add(key: String, value: [Int?]) -> Self {
        self.json[key] = JSON(value)
        return self
    }
    
    @discardableResult
    public func add(key: String, value: Double) -> Self {
        self.json[key] = JSON(Decimal(value))
        return self
    }
    
    @discardableResult
    public func add(key: String, value: Double?) -> Self {
        if let value {
            self.json[key] = JSON(Decimal(value))
        } else {
            self.json[key] = JSON.null
        }
        return self
    }
    
    @discardableResult
    public func add(key: String, value: [Double]) -> Self {
        self.json[key] = JSON(value.map({ Double($0) }))
        return self
    }
    
    @discardableResult
    public func add(key: String, value: [Double?]) -> Self {
        self.json[key] = JSON(value.map({ $0 == nil ? nil : Double($0!) }))
        return self
    }
    
    @discardableResult
    public func add(key: String, value: Bool) -> Self {
        self.json[key] = JSON(value)
        return self
    }
    
    @discardableResult
    public func add(key: String, value: Bool?) -> Self {
        self.json[key] = JSON(value ?? JSON.null)
        return self
    }
    
    @discardableResult
    public func add(key: String, value: [Bool]) -> Self {
        self.json[key] = JSON(value)
        return self
    }
    
    @discardableResult
    public func add(key: String, value: [Bool?]) -> Self {
        self.json[key] = JSON(value)
        return self
    }
    
    @discardableResult
    public func add(key: String, value: Date) -> Self {
        return self.add(key: key, value: self.dateFormatter.string(from: value))
    }
    
    @discardableResult
    public func add(key: String, value: Date?) -> Self {
        if let value {
            self.add(key: key, value: value)
        } else {
            self.json[key] = JSON.null
        }
        return self
    }
    
    @discardableResult
    public func add(key: String, value: [Date]) -> Self {
        let dateStrings: [String] = value.map({ self.dateFormatter.string(from: $0) })
        return self.add(key: key, value: dateStrings)
    }
    
    @discardableResult
    public func add(key: String, value: [Date?]) -> Self {
        let dateStrings: [String?] = value.map({
            if let date = $0 {
                return self.dateFormatter.string(from: date)
            } else {
                return nil
            }
        })
        return self.add(key: key, value: dateStrings)
    }
    
    @discardableResult
    public func add<T: Storable>(key: String, value: T) -> Self {
        self.json[key] = value.toDataObject().json
        return self
    }
    
    @discardableResult
    public func add<T: Storable>(key: String, value: T?) -> Self {
        self.json[key] = value?.toDataObject().json ?? JSON.null
        return self
    }
    
    @discardableResult
    public func add<T: Storable>(key: String, value: [T]) -> Self {
        self.json[key] = JSON(value.map { $0.toDataObject().json })
        return self
    }
    
    @discardableResult
    public func add<T: Storable>(key: String, value: [T?]) -> Self {
        self.json[key] = JSON(value.map { $0?.toDataObject().json ?? JSON.null })
        return self
    }
    
    // MARK: - Data retrieval methods
    
    public func get(_ key: String, onFail: String = "", legacyKeys: [String] = []) -> String {
        var retrieval = self.json[key].string
        for legacyKey in legacyKeys {
            guard retrieval == nil else { break }
            retrieval = self.json[legacyKey].string
        }
        assert(retrieval != nil, "Failed to restore attribute '\(key)' to object '\(self.objectName)'")
        return retrieval ?? onFail
    }
    
    public func get(_ key: String, legacyKeys: [String] = []) -> String? {
        var retrieval = self.json[key].string
        for legacyKey in legacyKeys {
            guard retrieval == nil else { break }
            retrieval = self.json[legacyKey].string
        }
        return retrieval
    }
    
    public func get(_ key: String, legacyKeys: [String] = []) -> [String] {
        var retrieval = self.json[key].array
        for legacyKey in legacyKeys {
            guard retrieval == nil else { break }
            retrieval = self.json[legacyKey].array
        }
        assert(retrieval != nil, "Failed to restore attribute '\(key)' to object '\(self.objectName)'")
        let result = (retrieval ?? []).compactMap({ $0.string })
        assert(retrieval?.count == result.count, "JSON array came with \(retrieval?.count ?? -1) elements, but only \(result.count) could be restored")
        return result
    }
    
    public func get(_ key: String, legacyKeys: [String] = []) -> [String?] {
        var retrieval = self.json[key].array
        for legacyKey in legacyKeys {
            guard retrieval == nil else { break }
            retrieval = self.json[legacyKey].array
        }
        assert(retrieval != nil, "Failed to restore attribute '\(key)' to object '\(self.objectName)'")
        let result = (retrieval ?? []).map({ $0.string })
        return result
    }
    
    public func get(_ key: String, onFail: Int = 0, legacyKeys: [String] = []) -> Int {
        var retrieval = self.json[key].int
        for legacyKey in legacyKeys {
            guard retrieval == nil else { break }
            retrieval = self.json[legacyKey].int
        }
        assert(retrieval != nil, "Failed to restore attribute '\(key)' to object '\(self.objectName)'")
        return retrieval ?? onFail
    }
    
    public func get(_ key: String, legacyKeys: [String] = []) -> Int? {
        var retrieval = self.json[key].int
        for legacyKey in legacyKeys {
            guard retrieval == nil else { break }
            retrieval = self.json[legacyKey].int
        }
        return retrieval
    }
    
    public func get(_ key: String, legacyKeys: [String] = []) -> [Int] {
        var retrieval = self.json[key].array
        for legacyKey in legacyKeys {
            guard retrieval == nil else { break }
            retrieval = self.json[legacyKey].array
        }
        assert(retrieval != nil, "Failed to restore attribute '\(key)' to object '\(self.objectName)'")
        let result = (retrieval ?? []).compactMap({ $0.int })
        assert(retrieval?.count == result.count, "JSON array came with \(retrieval?.count ?? -1) elements, but only \(result.count) could be restored")
        return result
    }
    
    public func get(_ key: String, legacyKeys: [String] = []) -> [Int?] {
        var retrieval = self.json[key].array
        for legacyKey in legacyKeys {
            guard retrieval == nil else { break }
            retrieval = self.json[legacyKey].array
        }
        assert(retrieval != nil, "Failed to restore attribute '\(key)' to object '\(self.objectName)'")
        let result = (retrieval ?? []).map({ $0.int })
        return result
    }
    
    public func get(_ key: String, onFail: Double = 0.0, legacyKeys: [String] = []) -> Double {
        var retrieval = self.json[key].double
        for legacyKey in legacyKeys {
            guard retrieval == nil else { break }
            retrieval = self.json[legacyKey].double
        }
        assert(retrieval != nil, "Failed to restore attribute '\(key)' to object '\(self.objectName)'")
        return retrieval ?? onFail
    }
    
    public func get(_ key: String, legacyKeys: [String] = []) -> Double? {
        var retrieval = self.json[key].double
        for legacyKey in legacyKeys {
            guard retrieval == nil else { break }
            retrieval = self.json[legacyKey].double
        }
        return retrieval
    }
    
    public func get(_ key: String, legacyKeys: [String] = []) -> [Double] {
        var retrieval = self.json[key].array
        for legacyKey in legacyKeys {
            guard retrieval == nil else { break }
            retrieval = self.json[legacyKey].array
        }
        assert(retrieval != nil, "Failed to restore attribute '\(key)' to object '\(self.objectName)'")
        let result = (retrieval ?? []).compactMap({ $0.double })
        assert(retrieval?.count == result.count, "JSON array came with \(retrieval?.count ?? -1) elements, but only \(result.count) could be restored")
        return result
    }
    
    public func get(_ key: String, legacyKeys: [String] = []) -> [Double?] {
        var retrieval = self.json[key].array
        for legacyKey in legacyKeys {
            guard retrieval == nil else { break }
            retrieval = self.json[legacyKey].array
        }
        assert(retrieval != nil, "Failed to restore attribute '\(key)' to object '\(self.objectName)'")
        let result = (retrieval ?? []).map({ $0.double })
        return result
    }
    
    public func get(_ key: String, onFail: Bool, legacyKeys: [String] = []) -> Bool {
        var retrieval = self.json[key].bool
        for legacyKey in legacyKeys {
            guard retrieval == nil else { break }
            retrieval = self.json[legacyKey].bool
        }
        assert(retrieval != nil, "Failed to restore attribute '\(key)' to object '\(self.objectName)'")
        return retrieval ?? onFail
    }
    
    public func get(_ key: String, legacyKeys: [String] = []) -> Bool? {
        var retrieval = self.json[key].bool
        for legacyKey in legacyKeys {
            guard retrieval == nil else { break }
            retrieval = self.json[legacyKey].bool
        }
        return retrieval
    }
    
    public func get(_ key: String, legacyKeys: [String] = []) -> [Bool] {
        var retrieval = self.json[key].array
        for legacyKey in legacyKeys {
            guard retrieval == nil else { break }
            retrieval = self.json[legacyKey].array
        }
        assert(retrieval != nil, "Failed to restore attribute '\(key)' to object '\(self.objectName)'")
        let result = (retrieval ?? []).compactMap({ $0.bool })
        assert(retrieval?.count == result.count, "JSON array came with \(retrieval?.count ?? -1) elements, but only \(result.count) could be restored")
        return result
    }
    
    public func get(_ key: String, legacyKeys: [String] = []) -> [Bool?] {
        var retrieval = self.json[key].array
        for legacyKey in legacyKeys {
            guard retrieval == nil else { break }
            retrieval = self.json[legacyKey].array
        }
        assert(retrieval != nil, "Failed to restore attribute '\(key)' to object '\(self.objectName)'")
        let result = (retrieval ?? []).map({ $0.bool })
        return result
    }
    
    public func get(_ key: String, onFail: Date = Date(), legacyKeys: [String] = []) -> Date {
        let dateString: String = self.get(key, onFail: "", legacyKeys: legacyKeys)
        if dateString.isEmpty {
            assertionFailure("Failed to restore attribute '\(key)' to object \(self.objectName)")
            return onFail
        }
        let date = self.dateFormatter.date(from: dateString)
        assert(date != nil, "Failed to restore attribute '\(key)' to object '\(self.objectName)'")
        return date ?? onFail
    }
    
    public func get(_ key: String, legacyKeys: [String] = []) -> Date? {
        let dateString: String? = self.get(key, legacyKeys: legacyKeys)
        guard dateString != nil else {
            return nil
        }
        return self.dateFormatter.date(from: dateString!)
    }
    
    public func get(_ key: String, legacyKeys: [String] = []) -> [Date] {
        var retrieval = self.json[key].array
        for legacyKey in legacyKeys {
            guard retrieval == nil else { break }
            retrieval = self.json[legacyKey].array
        }
        assert(retrieval != nil, "Failed to restore attribute '\(key)' to object '\(self.objectName)'")
        let stringResult = (retrieval ?? []).compactMap({ $0.string })
        let result = stringResult.compactMap({ self.dateFormatter.date(from: $0) })
        assert(retrieval?.count == result.count, "JSON array came with \(retrieval?.count ?? -1) elements, but only \(result.count) could be restored")
        return result
    }
    
    public func get(_ key: String, legacyKeys: [String] = []) -> [Date?] {
        var retrieval = self.json[key].array
        for legacyKey in legacyKeys {
            guard retrieval == nil else { break }
            retrieval = self.json[legacyKey].array
        }
        assert(retrieval != nil, "Failed to restore attribute '\(key)' to object '\(self.objectName)'")
        let stringResult = (retrieval ?? []).map({ $0.string })
        let result = stringResult.map({
            if let stringDate = $0 {
                return self.dateFormatter.date(from: stringDate)
            } else {
                return nil
            }
        })
        return result
    }
    
    public func getObject<T>(_ key: String, type: T.Type, legacyKeys: [String] = []) -> T where T: Storable {
        var json = self.json[key]
        for legacyKey in legacyKeys {
            guard json == JSON.null else { break }
            json = self.json[legacyKey]
        }
        return DataObject(json: JSON(json.object)).restore(type)
    }
    
    public func getObjectOptional<T>(_ key: String, type: T.Type, legacyKeys: [String] = []) -> T? where T: Storable {
        var json = self.json[key]
        for legacyKey in legacyKeys {
            guard json == JSON.null else { break }
            json = self.json[legacyKey]
        }
        if json == JSON.null { return nil }
        return DataObject(json: JSON(json.object)).restoreOptional(type)
    }
    
    public func getObjectArray<T>(_ key: String, type: T.Type, legacyKeys: [String] = []) -> [T] where T: Storable {
        var retrieval = self.json[key].array
        for legacyKey in legacyKeys {
            guard retrieval == nil else { break }
            retrieval = self.json[legacyKey].array
        }
        assert(retrieval != nil, "Failed to restore attribute '\(key)' to object '\(self.objectName)'")
        return ((retrieval ?? []).map { DataObject(json: $0) }).restoreArray(type)
    }
    
    // MARK: - String export
    
    public func toRawString() -> String? {
        return self.json.rawString()
    }
    
    // MARK: - Storable export
    
    internal func restore<T>(_ type: T.Type) -> T where T: Storable {
        let parse = self.parse()
        guard let object = parse as? T else {
            fatalError("Object \(type.self) could not be restored - some class within its inheritance tree likely forgot to add a variable within the toDataObject call")
        }
        return object
    }
    
    internal func restoreOptional<T>(_ type: T.Type) -> T? where T: Storable {
        return self.parse() as? T
    }
    
    private func parse() -> Storable? {
        if let className = self.json[self.objectField].string {
            var activeClassName = className
            while Legacy.newClassNames[activeClassName] != nil {
                activeClassName = Legacy.newClassNames[activeClassName]!
            }
            let nameSpace = Bundle.main.infoDictionary!["CFBundleExecutable"] as! String
            var type = NSClassFromString("\(nameSpace).\(activeClassName)") as? Storable.Type
            if type == nil {
                // If the type doesn't exist, we may be looking at the wrong namespace - check package namespace instead
                type = NSClassFromString("SwiftSerialization.\(activeClassName)") as? Storable.Type
            }
            guard let type else {
                assertionFailure("Class \(nameSpace).\(activeClassName) does not exist but is trying to be restored")
                return nil
            }
            return type.init(dataObject: DataObject(json: self.json))
        }
        return nil
    }
    
}

extension Array where Element: DataObject {
    
    fileprivate func restoreArray<T>(_ type: T.Type) -> [T] where T: Storable {
        var restored = Array<T>()
        for element in self {
            guard let restoredElement = element.restoreOptional(T.self) else {
                assertionFailure("DataObject of type \(String(describing: T.self)) could not be restored")
                continue
            }
            restored.append(restoredElement)
        }
        return restored
    }
    
}
