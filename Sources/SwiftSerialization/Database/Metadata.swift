//
//  Metadata.swift
//  SwiftLocal
//
//  Created by Andre Pham on 3/1/2023.
//

import Foundation

internal class Metadata: Storable {
    
    internal let id: String
    internal let objectName: String
    internal let createdAt: Date
    
    internal init(objectName: String, id: String) {
        self.objectName = objectName
        self.id = id
        self.createdAt = Date.now
    }
    
    // MARK: - Serialization

    private enum Field: String {
        case id
        case objectName
        case createdAt
    }

    required internal init(dataObject: DataObject) {
        self.id = dataObject.get(Field.id.rawValue)
        self.objectName = dataObject.get(Field.objectName.rawValue)
        self.createdAt = dataObject.get(Field.createdAt.rawValue)
    }

    internal func toDataObject() -> DataObject {
        return DataObject(self)
            .add(key: Field.id.rawValue, value: self.id)
            .add(key: Field.objectName.rawValue, value: self.objectName)
            .add(key: Field.createdAt.rawValue, value: self.createdAt)
    }
    
}
