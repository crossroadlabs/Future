//===--- Future.swift ------------------------------------------------------===//
//Copyright (c) 2016 Daniel Leping (dileping)
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
//===----------------------------------------------------------------------===//

import Foundation

import Result
import Boilerplate
import ExecutionContext

public protocol FutureProtocol : MovableExecutionContextTenantProtocol {
    associatedtype Value
    
    init(value:Value)
    init(error:Error)
    init<E : Error>(result:Result<Value, E>)
    
    @discardableResult
    func onComplete<E: Error>(_ callback: @escaping (Result<Value, E>) -> Void) -> Self
    
    var isCompleted:Bool {get}
}

public class Future<V> : FutureProtocol {
    public typealias Value = V
    
    private var _chain:TaskChain?
    private var _resolver:ExecutionContextProtocol?
    internal var result:Result<Value, AnyError>? = nil {
        didSet {
            if nil != result {
                self.isCompleted = true
                
                /// some performance optimization is done here, so don't touch the ifs. ExecutionContext.current is not the fastest func
                let context = selectContext()
                let chain = _chain!
                
                admin.execute {
                    chain.append { next in
                        return { context in
                            admin.execute {
                                self._resolver = context
                                self._chain = nil
                                context.execute {
                                    next.content?(context)
                                }
                            }
                        }
                    }
                    chain.perform(in: context)
                }
            }
        }
    }
    
    public let context: ExecutionContextProtocol
    //make it atomic later
    private (set) public var isCompleted:Bool = false
    
    internal init(context:ExecutionContextProtocol) {
        self._chain = TaskChain()
        self.context = context
    }
    
    public convenience init(context:ExecutionContextProtocol, value:Value) {
        self.init(context: context, result: Result<Value, AnyError>(value: value))
    }
    
    public convenience init(context:ExecutionContextProtocol, error:Error) {
        self.init(context: context, result: Result(error: AnyError(error)))
    }
    
    public convenience init<E : Error>(context:ExecutionContextProtocol, result:Result<Value, E>) {
        self.init(context: context)
        self.result = result.asAnyError()
        self.isCompleted = true
        self._resolver = selectContext()
        self._chain = nil
    }
    
    public required convenience init(value:Value) {
        self.init(result: Result<Value, AnyError>(value: value))
    }
    
    public required convenience init(error:Error) {
        self.init(result: Result(error: AnyError(error)))
    }
    
    public required convenience init<E : Error>(result:Result<Value, E>) {
        self.init(context: immediate, result: result)
    }
    
    private func selectContext() -> ExecutionContextProtocol {
        return self.context.isEqual(to: immediate) ? ExecutionContext.current : self.context
    }
    
    @discardableResult
    public func onComplete<E: Error>(_ callback: @escaping (Result<Value, E>) -> Void) -> Self {
        return self.onCompleteInternal(callback: callback)
    }
    
    //to avoid endless recursion
    internal func onCompleteInternal<E: Error>(callback: @escaping (Result<Value, E>) -> Void) -> Self {
        admin.execute {
            if let resolver = self._resolver {
                let mapped:Result<Value, E>? = self.result!.tryAsError()
                if let result = mapped {
                    resolver.execute {
                        callback(result)
                    }
                }
            } else {
                self._chain!.append { next in
                    return { context in
                        let mapped:Result<Value, E>? = self.result!.tryAsError()
                        
                        if let result = mapped {
                            callback(result)
                            next.content?(context)
                        } else {
                            next.content?(context)
                        }
                    }
                }
            }
        }
        
        return self
    }
}

extension Future : MovableExecutionContextTenantProtocol {
    public typealias SettledTenant = Future<Value>
    
    public func settle(in context: ExecutionContextProtocol) -> SettledTenant {
        let future = MutableFuture<Value>(context: context)
        
        future.completeWith(future: self)
        
        return future
    }
}

public func future<T>(context:ExecutionContextProtocol = contextSelector(), _ task:@escaping () throws ->T) -> Future<T> {
    let future = MutableFuture<T>(context: context)
    
    context.execute {
        do {
            let value = try task()
            try! future.success(value: value)
        } catch let e {
            try! future.fail(error: e)
        }
    }
    
    return future
}

public func future<T, E : Error>(context:ExecutionContextProtocol = contextSelector(), _ task:@escaping () -> Result<T, E>) -> Future<T> {
    let future = MutableFuture<T>(context: context)
    
    context.execute {
        let result = task()
        try! future.complete(result: result)
    }
    
    return future
}

public func future<T, F : FutureProtocol>(context:ExecutionContextProtocol = contextSelector(), _ task:@escaping () -> F) -> Future<T> where F.Value == T {
    let future = MutableFuture<T>(context: context)
    
    context.execute {
        future.completeWith(future: task())
    }
    
    return future
}
