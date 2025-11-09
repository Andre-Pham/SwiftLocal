//
//  Metadata.swift
//  SwiftLocal
//
//  Created by Andre Pham on 3/1/2023.
//

import Foundation

internal class Metadata: Storable {
    // MARK: Nested Types

    private enum Field: String {
        case id
        case objectName
    }

    // MARK: Properties

    internal let id: String
    internal let objectName: String

    // MARK: Lifecycle

    internal init(objectName: String, id: String) {
        self.objectName = objectName
        self.id = id
    }

    internal required init(dataObject: DataObject) {
        self.id = dataObject.get(Field.id.rawValue)
        self.objectName = dataObject.get(Field.objectName.rawValue)
    }

    // MARK: Functions

    internal func toDataObject() -> DataObject {
        return DataObject(self)
            .add(key: Field.id.rawValue, value: self.id)
            .add(key: Field.objectName.rawValue, value: self.objectName)
    }
}
