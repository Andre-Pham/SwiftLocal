//
//  Storable.swift
//  SwiftLocal
//
//  Created by Andre Pham on 6/11/2022.
//

import Foundation

/// Refer to README.md for usage examples.
public protocol Storable {
    
    func toDataObject() -> DataObject
    init(dataObject: DataObject)
    
}
extension Storable {
    
    internal var className: String {
        return String(describing: type(of: self))
    }
    
}
