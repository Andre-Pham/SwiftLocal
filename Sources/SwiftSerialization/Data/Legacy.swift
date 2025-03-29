//
//  Legacy.swift
//  SwiftSerialization
//
//  Created by Andre Pham on 23/2/2023.
//

import Foundation

/// Your application should always populate Legacy with the history of class name refactors. This way legacy class names can be accessed during database reads.
/// This should be executed somewhere in the application startup.
public enum Legacy {
    
    /// A dictionary of class names that may be stored and have been since refactored to a new name
    static private(set) var newClassNames: [String: String] = [:] // old: new
    /// A dictionary of class names and their legacy counterparts
    static private(set) var oldClassNames: [String: [String]] = [:] // new: old
    
    /// Add a class name refactoring to consider when restoring objects that were written before the refactor.
    /// - Parameters:
    ///   - old: The old class name
    ///   - new: The new class name
    public static func addClassRefactor(old: String, new: String) {
        Self.newClassNames[old] = new
        if Self.oldClassNames[new] != nil && !(Self.oldClassNames[new]!.contains(old)) {
            Self.oldClassNames[new]!.append(old)
        } else {
            Self.oldClassNames[new] = [old]
        }
    }
    
}
