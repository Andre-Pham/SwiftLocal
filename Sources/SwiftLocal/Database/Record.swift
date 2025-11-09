//
//  Record.swift
//  SwiftLocal
//
//  Created by Andre Pham on 3/1/2023.
//

import Foundation

public class Record<T: Storable> {
    // MARK: Properties

    internal let metadata: Metadata
    internal let data: T

    // MARK: Lifecycle

    public init(id: String = UUID().uuidString, data: T) {
        self.metadata = Metadata(objectName: data.className, id: id)
        self.data = data
    }
}
