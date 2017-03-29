//===--- Signal.swift ------------------------------------------------------===//
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

import Boilerplate
import Result
import Event

public extension SignalStream {
    public func flatMap<A, F : FutureProtocol>(_ f:@escaping (Payload)->F) -> SignalStream<Result<A, AnyError>> where F.Value == A, F.SettledTenant == Future<A> {
        let context = self.context
        return SignalStream<Result<A, AnyError>>(context: context, advise: { fun in
            self.chain { sig, payload in
                f(payload).settle(in: context).onComplete { (result:Result<A, AnyError>) in
                    fun((sig, result))
                }
            }
        })
    }
}

public extension SignalEndpoint {
    public func subscribe<F : FutureProtocol>(to future: F) -> Off where F.Value == Payload {
        let token = InvalidationToken()
        var endpoint:Self? = self
        future.onSuccess(token: token) { value in
            endpoint?.signal(signature: [], payload: value)
        }
        return {
            token.valid = false
            endpoint = nil
        }
    }
}

extension ResultProtocol {
    init(result:Result<Value, Error>) {
        guard let value = result.value else {
            guard let error = result.error else {
                fatalError("WTF?")
                
            }
            
            self.init(error: error)
            return
        }
        
        self.init(value: value)
    }
}

public extension SignalEndpoint where Payload : ResultProtocol, Payload.Error == AnyError {
    public func subscribe<F : FutureProtocol>(to future: F) -> Off where F.Value == Payload.Value {
        let token = InvalidationToken()
        var endpoint:Self? = self
        future.onComplete { (result:Result<Payload.Value, AnyError>) in
            endpoint?.signal(signature: [], payload: Payload(result: result))
        }
        return {
            token.valid = false
            endpoint = nil
        }
    }
}

public extension SignalEndpoint where Payload : Error {
    public func subscribe<F : FutureProtocol>(to future: F) -> Off {
        let token = InvalidationToken()
        var endpoint:Self? = self
        future.onFailure(token: token) { (error:Payload) in
            endpoint?.signal(signature: [], payload: error)
        }
        return {
            token.valid = false
            endpoint = nil
        }
    }
}

public extension FutureProtocol {
    public func pour<SE : SignalEndpoint>(to endpoint: SE) -> Off where SE.Payload == Value {
        return endpoint.subscribe(to: self)
    }
    
    public func pour<SE : SignalEndpoint>(to endpoint: SE) -> Off where SE.Payload : Error {
        return endpoint.subscribe(to: self)
    }
    
    public func pour<SE : SignalEndpoint>(to endpoint: SE) -> Off where SE.Payload : ResultProtocol, SE.Payload.Value == Value, SE.Payload.Error == AnyError {
        return endpoint.subscribe(to: self)
    }
}

public func <= <SE : SignalEndpoint>(endpoint:SE, future:Future<SE.Payload>) {
    let _ = future.pour(to: endpoint)
}

public func <= <SE : SignalEndpoint>(endpoint:SE, future:Future<SE.Payload.Value>) where SE.Payload : ResultProtocol, SE.Payload.Error == AnyError {
    let _ = future.pour(to: endpoint)
}

public func <= <SE : SignalEndpoint, F : FutureProtocol>(endpoint:SE, future:F) where SE.Payload : Error {
    let _ = future.pour(to: endpoint)
}

public func => <SE : SignalEndpoint>(future:Future<SE.Payload>, endpoint:SE) {
    endpoint <= future
}

public func => <SE : SignalEndpoint>(future:Future<SE.Payload.Value>, endpoint:SE) where SE.Payload : ResultProtocol, SE.Payload.Error == AnyError {
    endpoint <= future
}

public func => <SE : SignalEndpoint, F : FutureProtocol>(future:F, endpoint:SE) where SE.Payload : Error {
    endpoint <= future
}
