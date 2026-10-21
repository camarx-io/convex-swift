import Combine
import Foundation
import XCTest

@testable import ConvexMobile
@testable import UniFFI

final class ConvexMobileTests: XCTestCase {
  func testSubscribeResult() async throws {
    let expectation = self.expectation(description: "subscribe")
    var error: ClientError?
    var result: Message?
    let client = ConvexMobile.ConvexClient(ffiClient: FakeMobileConvexClient())

    let cancellationHandle = client.subscribe(to: "foo").sink(
      receiveCompletion: { completion in
        switch completion {
        case .finished:
          break
        case .failure(let clientError):
          error = clientError
          break
        }
      },
      receiveValue: { (value: Message) in
        result = value
        expectation.fulfill()
      })

    await fulfillment(of: [expectation], timeout: 10)

    XCTAssertEqual(result!.id, "the_id")
    XCTAssertEqual(result!.val, 42)
    XCTAssertNil(error)
  }

  func testSubscribeCanTrimDupeVals() async throws {
    let receivedSomething = self.expectation(description: "subscribe")
    let donePublishing = self.expectation(description: "done_publishing")
    var error: ClientError?
    var result: [Message] = []
    let client = ConvexMobile.ConvexClient(
      ffiClient: FakeMobileConvexClient(resultPublished: donePublishing))

    let cancellationHandle = client.subscribe(to: "dupeVals")
      .removeDuplicates()
      .sink(
        receiveCompletion: { completion in
          switch completion {
          case .finished:
            break
          case .failure(let clientError):
            error = clientError
            break
          }
        },
        receiveValue: { (value: Message) in
          result.append(value)
          if result.count == 1 {
            receivedSomething.fulfill()
          }
        })

    await fulfillment(of: [donePublishing, receivedSomething], timeout: 10)

    XCTAssertEqual(result.count, 1)
    XCTAssertNil(error)
  }

  func testSubscribeOptionalResultWithPresentVal() async throws {
    let expectation = self.expectation(description: "subscribe")
    var error: ClientError?
    var result: MessageWithOptionalVal?
    let client = ConvexMobile.ConvexClient(ffiClient: FakeMobileConvexClient())

    let cancellationHandle = client.subscribe(to: "foo").sink(
      receiveCompletion: { completion in
        switch completion {
        case .finished:
          break
        case .failure(let clientError):
          error = clientError
          break
        }
      },
      receiveValue: { (value: MessageWithOptionalVal?) in
        result = value
        expectation.fulfill()
      })

    await fulfillment(of: [expectation], timeout: 10)

    XCTAssertEqual(result!.id, "the_id")
    XCTAssertEqual(result!.val, 42)
    XCTAssertNil(error)
  }

  func testSubscribeOptionalResultWithNullVal() async throws {
    let expectation = self.expectation(description: "subscribe")
    var error: ClientError?
    var result: MessageWithOptionalVal?
    let client = ConvexMobile.ConvexClient(ffiClient: FakeMobileConvexClient())

    let cancellationHandle = client.subscribe(to: "nullVal").sink(
      receiveCompletion: { completion in
        switch completion {
        case .finished:
          break
        case .failure(let clientError):
          error = clientError
          break
        }
      },
      receiveValue: { (value: MessageWithOptionalVal?) in
        result = value
        expectation.fulfill()
      })

    await fulfillment(of: [expectation], timeout: 10)

    XCTAssertEqual(result!.id, "the_id")
    XCTAssertEqual(result!.val, nil)
    XCTAssertNil(error)
  }

  func testSubscribeOptionalResultWithMissingVal() async throws {
    let expectation = self.expectation(description: "subscribe")
    var error: ClientError?
    var result: MessageWithOptionalVal?
    let client = ConvexMobile.ConvexClient(ffiClient: FakeMobileConvexClient())

    let cancellationHandle = client.subscribe(to: "missingVal").sink(
      receiveCompletion: { completion in
        switch completion {
        case .finished:
          break
        case .failure(let clientError):
          error = clientError
          break
        }
      },
      receiveValue: { (value: MessageWithOptionalVal?) in
        result = value
        expectation.fulfill()
      })

    await fulfillment(of: [expectation], timeout: 10)

    XCTAssertEqual(result!.id, "the_id")
    XCTAssertEqual(result!.val, nil)
    XCTAssertNil(error)
  }

  func testMissingSubscribeArgs() async throws {
    let expectation = self.expectation(description: "subscribe")
    let fakeFfiClient = FakeMobileConvexClient()
    let client = ConvexMobile.ConvexClient(ffiClient: fakeFfiClient)

    let cancellationHandle = try client.subscribe(to: "foo").sink(
      receiveCompletion: { completion in
        switch completion {
        case .finished:
          break
        case .failure:
          break
        }
      },
      receiveValue: { (value: Message) in
        expectation.fulfill()
      })

    await fulfillment(of: [expectation], timeout: 10)

    XCTAssertEqual(fakeFfiClient.subscriptionArgs, [:])
  }

  func testPopulatedSubscribeArgs() async throws {
    let expectation = self.expectation(description: "subscribe")
    let fakeFfiClient = FakeMobileConvexClient()
    let client = ConvexMobile.ConvexClient(ffiClient: fakeFfiClient)

    let cancellationHandle = client.subscribe(
      to: "foo",
      with: [
        "aString": "bar", "aDouble": 42.0, "anInt": 42, "aNil": nil,
        "aDict": ["sub1": 1.0, "nested": ["ohmy": true]], "aList": [true, false, true, nil],
      ]
    ).sink(
      receiveCompletion: { completion in
        switch completion {
        case .finished:
          break
        case .failure:
          break
        }
      }) {
        (value: Message) in
        expectation.fulfill()
      }

    await fulfillment(of: [expectation], timeout: 10)

    XCTAssertEqual(fakeFfiClient.subscriptionArgs["aString"], "\"bar\"")
    XCTAssertEqual(fakeFfiClient.subscriptionArgs["aDouble"], "42")
    XCTAssertEqual(fakeFfiClient.subscriptionArgs["anInt"], "{\"$integer\":\"KgAAAAAAAAA=\"}")
    XCTAssertEqual(fakeFfiClient.subscriptionArgs["aNil"], "null")
    XCTAssertEqual(
      fakeFfiClient.subscriptionArgs["aDict"], "{\"nested\":{\"ohmy\":true},\"sub1\":1}")
    XCTAssertEqual(fakeFfiClient.subscriptionArgs["aList"], "[true,false,true,null]")

  }

  func testSubscribeCancellation() async throws {
    let expectation = self.expectation(description: "subscribe")
    var error: ClientError?
    let ffiClient = FakeMobileConvexClient()
    let client = ConvexMobile.ConvexClient(ffiClient: ffiClient)

    let cancellationHandle = client.subscribe(to: "foo").sink(
      receiveCompletion: { completion in
        switch completion {
        case .finished:
          break
        case .failure(let clientError):
          error = clientError
          break
        }
      },
      receiveValue: { (value: Message) in
        expectation.fulfill()
      })

    await fulfillment(of: [expectation], timeout: 10)
    cancellationHandle.cancel()

    XCTAssertEqual(ffiClient.cancellationCount, 1)
    XCTAssertNil(error)
  }

  func testSubscriptionDeliversUpdateBeforeHandleResolves() async throws {
    let receivedValue = expectation(description: "received synchronous subscription update")
    let ffiClient = FakeMobileConvexClient(
      controlledSubscriptions: true, updateBeforeSubscriptionReturns: true)
    let client = ConvexMobile.ConvexClient(ffiClient: ffiClient)
    var result: Message?

    let cancellable = client.subscribe(to: "cached", yielding: Message.self)
      .sink(
        receiveCompletion: { _ in },
        receiveValue: { value in
          result = value
          receivedValue.fulfill()
        })

    await fulfillment(of: [receivedValue], timeout: 0.5)
    cancellable.cancel()

    XCTAssertEqual(result?.id, "the_id")
    XCTAssertEqual(result?.val, 42)
  }

  func testSubscriptionRaceConcurrentCallbacksAndCancellation() async throws {
    let iterationCount = 2_000
    let ffiClient = FakeMobileConvexClient(controlledSubscriptions: true)
    let client = ConvexMobile.ConvexClient(ffiClient: ffiClient)
    let receivedValues = LockedValue(0)
    let valuesReceivedAfterCancel = LockedValue(0)
    let cancellationReturned = LockedValue(Array(repeating: false, count: iterationCount))
    var cancellables: [AnyCancellable] = []
    cancellables.reserveCapacity(iterationCount)

    for index in 0..<iterationCount {
      let name = String(format: "race-%04d", index)
      let cancellable = client.subscribe(to: name, yielding: Message.self)
        .receive(on: DispatchQueue.main)
        .map { value in
          Just(value)
            .setFailureType(to: ClientError.self)
            .eraseToAnyPublisher()
        }
        .switchToLatest()
        .sink(
          receiveCompletion: { _ in },
          receiveValue: { _ in
            receivedValues.withValue { $0 += 1 }
            if cancellationReturned.withValue({ $0[index] }) {
              valuesReceivedAfterCancel.withValue { $0 += 1 }
            }
          })
      cancellables.append(cancellable)
    }

    guard await waitUntil({ ffiClient.subscriptionCount == iterationCount }) else {
      XCTFail("Timed out waiting for all race subscriptions to start")
      return
    }

    let subscribers = ffiClient.capturedSubscribers
    let handles = ffiClient.subscriptionHandles
    XCTAssertEqual(subscribers.count, iterationCount)
    XCTAssertEqual(handles.count, iterationCount)

    // Prove every Future has resolved and FlatMap has subscribed to its HandleEvents publisher
    // before racing Rust-style callbacks against downstream cancellation.
    for subscriber in subscribers {
      subscriber.onUpdate(value: FakeMobileConvexClient.messageJSON)
    }
    guard await waitUntil(timeout: 5, { receivedValues.value == iterationCount }) else {
      XCTFail("Timed out waiting for all race subscriptions to become active")
      return
    }

    DispatchQueue.concurrentPerform(iterations: iterationCount * 3) { operation in
      let index = operation / 3
      switch operation % 3 {
      case 0:
        subscribers[index].onUpdate(value: FakeMobileConvexClient.messageJSON)
      case 1:
        subscribers[index].onError(message: "race error", value: nil)
      default:
        cancellables[index].cancel()
        cancellationReturned.withValue { $0[index] = true }
      }
    }

    // Exercise callbacks that originate strictly after every cancel() call has returned.
    DispatchQueue.concurrentPerform(iterations: iterationCount) { index in
      subscribers[index].onUpdate(value: FakeMobileConvexClient.messageJSON)
    }
    try await Task.sleep(nanoseconds: 100_000_000)

    let incorrectlyCancelled = handles.filter { $0.cancelCount != 1 }
    XCTAssertTrue(
      incorrectlyCancelled.isEmpty,
      "Expected every raced subscription handle to be cancelled exactly once; "
        + "\(incorrectlyCancelled.count) of \(iterationCount) were not")
    XCTAssertEqual(
      valuesReceivedAfterCancel.value, 0,
      "Values were delivered after downstream cancellation returned")
  }

  func testSubscriptionCancelBeforeHandleResolvesCancelsExactlyOnce() async throws {
    let resolutionGate = SubscriptionResolutionGate()
    let ffiClient = FakeMobileConvexClient(
      controlledSubscriptions: true,
      subscriptionResolutionGate: resolutionGate)
    let client = ConvexMobile.ConvexClient(ffiClient: ffiClient)

    let cancellable = client.subscribe(to: "delayed", yielding: Message.self)
      .sink(receiveCompletion: { _ in }, receiveValue: { _ in })

    cancellable.cancel()

    guard await waitUntil({ ffiClient.subscriptionCount == 1 }) else {
      XCTFail("Timed out waiting for delayed subscription to start")
      return
    }
    let handle = try XCTUnwrap(ffiClient.subscriptionHandles.first)

    resolutionGate.resolve()
    _ = await waitUntil(timeout: 0.5, { handle.cancelCount == 1 })

    XCTAssertEqual(
      handle.cancelCount, 1,
      "A subscription cancelled before initialization completed leaked its server handle")
  }

  func testSubscriptionCancelIsExactlyOnceAcrossCancellationPaths() async throws {
    let ffiClient = FakeMobileConvexClient(controlledSubscriptions: true)
    let client = ConvexMobile.ConvexClient(ffiClient: ffiClient)
    let activeValues = LockedValue(0)

    let activeCancellable = client.subscribe(to: "active", yielding: Message.self)
      .sink(
        receiveCompletion: { _ in },
        receiveValue: { _ in activeValues.withValue { $0 += 1 } })

    guard await waitUntil({ ffiClient.subscriptionCount == 1 }) else {
      XCTFail("Timed out waiting for active subscription")
      return
    }
    let activeSubscriber = ffiClient.capturedSubscribers[0]
    let activeHandle = ffiClient.subscriptionHandles[0]
    guard await publishUntilReceived(activeSubscriber, receivedValues: activeValues) else {
      XCTFail("Timed out activating subscription")
      return
    }

    activeCancellable.cancel()
    activeCancellable.cancel()
    XCTAssertEqual(activeHandle.cancelCount, 1, "Double cancel must cancel the handle only once")

    let errorCompletion = expectation(description: "subscription error")
    let erroredValues = LockedValue(0)
    let erroredCancellable = client.subscribe(to: "errored", yielding: Message.self)
      .sink(
        receiveCompletion: { completion in
          if case .failure = completion {
            errorCompletion.fulfill()
          }
        },
        receiveValue: { _ in erroredValues.withValue { $0 += 1 } })

    guard await waitUntil({ ffiClient.subscriptionCount == 2 }) else {
      XCTFail("Timed out waiting for errored subscription")
      return
    }
    let erroredSubscriber = ffiClient.capturedSubscribers[1]
    let erroredHandle = ffiClient.subscriptionHandles[1]
    guard await publishUntilReceived(erroredSubscriber, receivedValues: erroredValues) else {
      XCTFail("Timed out activating errored subscription")
      return
    }

    erroredSubscriber.onError(message: "server error", value: nil)
    await fulfillment(of: [errorCompletion], timeout: 2)
    erroredCancellable.cancel()
    erroredCancellable.cancel()
    _ = await waitUntil(timeout: 0.5, { erroredHandle.cancelCount == 1 })

    XCTAssertEqual(
      erroredHandle.cancelCount, 1,
      "Error completion must release the server handle, and later cancel must not release it twice")
  }

  func testSubscriptionRaceDoesNotDeliverValuesAfterCancelReturns() async throws {
    let ffiClient = FakeMobileConvexClient(controlledSubscriptions: true)
    let client = ConvexMobile.ConvexClient(ffiClient: ffiClient)
    let receivedValues = LockedValue(0)
    let cancellationReturned = LockedValue(false)
    let valuesReceivedAfterCancel = LockedValue(0)

    let cancellable = client.subscribe(to: "delivery", yielding: Message.self)
      .receive(on: DispatchQueue.main)
      .map { value in
        Just(value)
          .setFailureType(to: ClientError.self)
          .eraseToAnyPublisher()
      }
      .switchToLatest()
      .sink(
        receiveCompletion: { _ in },
        receiveValue: { _ in
          receivedValues.withValue { $0 += 1 }
          if cancellationReturned.value {
            valuesReceivedAfterCancel.withValue { $0 += 1 }
          }
        })

    guard await waitUntil({ ffiClient.subscriptionCount == 1 }) else {
      XCTFail("Timed out waiting for delivery subscription")
      return
    }
    let subscriber = ffiClient.capturedSubscribers[0]
    guard await publishUntilReceived(subscriber, receivedValues: receivedValues) else {
      XCTFail("Timed out activating delivery subscription")
      return
    }

    let callbacksStarted = DispatchSemaphore(value: 0)
    let allowPostCancelCallbacks = DispatchSemaphore(value: 0)
    let callbacksFinished = DispatchSemaphore(value: 0)
    DispatchQueue.global().async {
      callbacksStarted.signal()
      allowPostCancelCallbacks.wait()
      DispatchQueue.concurrentPerform(iterations: 2_000) { _ in
        subscriber.onUpdate(value: FakeMobileConvexClient.messageJSON)
      }
      callbacksFinished.signal()
    }

    callbacksStarted.wait()
    cancellable.cancel()
    cancellationReturned.withValue { $0 = true }
    allowPostCancelCallbacks.signal()
    callbacksFinished.wait()
    try await Task.sleep(nanoseconds: 100_000_000)

    XCTAssertEqual(
      valuesReceivedAfterCancel.value, 0,
      "The sink received values produced after cancel() returned")
  }

  func testMutationRoundTrip() async throws {
    let fakeFfiClient = FakeMobileConvexClient()
    let client = ConvexMobile.ConvexClient(ffiClient: fakeFfiClient)

    let message: Message = try await client.mutation("foo", with: ["anInt": 101])

    XCTAssertEqual(message.id, "the_id")
    XCTAssertEqual(message.val, 101)
  }

  func testVoidMutation() async throws {
    let fakeFfiClient = FakeMobileConvexClient()
    let client = ConvexMobile.ConvexClient(ffiClient: fakeFfiClient)

    try await client.mutation("nullResult")

    XCTAssertEqual(fakeFfiClient.mutationCalls, ["nullResult"])
  }

  func testActionRoundTrip() async throws {
    let fakeFfiClient = FakeMobileConvexClient()
    let client = ConvexMobile.ConvexClient(ffiClient: fakeFfiClient)

    let message: Message = try await client.action("foo", with: ["anInt": Int.max])

    XCTAssertEqual(message.id, "the_id")
    XCTAssertEqual(message.val, Int.max)
  }

  func testVoidAction() async throws {
    let fakeFfiClient = FakeMobileConvexClient()
    let client = ConvexMobile.ConvexClient(ffiClient: fakeFfiClient)

    try await client.action("nullResult")

    XCTAssertEqual(fakeFfiClient.actionCalls, ["nullResult"])
  }

  func testMutationTypeMismatchThrows() async throws {
    let client = ConvexMobile.ConvexClient(ffiClient: FakeMobileConvexClient())

    do {
      let _: Message = try await client.mutation("typeMismatch")
      XCTFail("Expected a DecodingError to be thrown")
    } catch is DecodingError {
      // Expected: decoding "just a plain string" into Message fails gracefully
    } catch {
      XCTFail("Expected DecodingError but got \(type(of: error)): \(error)")
    }
  }

  func testSubscribeTypeMismatchSendsError() async throws {
    let expectation = self.expectation(description: "subscribe error")
    var receivedError: ClientError?
    let client = ConvexMobile.ConvexClient(ffiClient: FakeMobileConvexClient())

    let cancellationHandle = client.subscribe(to: "typeMismatch").sink(
      receiveCompletion: { completion in
        switch completion {
        case .finished:
          break
        case .failure(let clientError):
          receivedError = clientError
          expectation.fulfill()
        }
      },
      receiveValue: { (value: Message) in
        XCTFail("Should not receive a value for a type mismatch")
      })

    await fulfillment(of: [expectation], timeout: 10)

    if case .InternalError = receivedError! {
      // Expected: decoding error surfaced as InternalError
    } else {
      XCTFail("Expected InternalError but got \(receivedError!)")
    }
  }

  func testLoginSetsAuthCallbackOnFfiClient() async throws {
    let fakeFfiClient = FakeMobileConvexClient()
    let client = ConvexMobile.ConvexClientWithAuth(
      ffiClient: fakeFfiClient, authProvider: FakeAuthProvider())

    let result = await client.login()

    XCTAssertEqual(try result.get(), FakeAuthProvider.CREDENTIALS)
    // The callback provider should have been set
    XCTAssertNotNil(fakeFfiClient.authProvider)
    // Fetching the token from the provider should return the extracted token
    let token = try await fakeFfiClient.authProvider?.fetchToken(forceRefresh: false)
    XCTAssertEqual(token, "extracted: \(FakeAuthProvider.CREDENTIALS)")
  }

  func testLoginFromCacheSetsAuthCallbackOnFfiClient() async throws {
    let fakeFfiClient = FakeMobileConvexClient()
    let client = ConvexMobile.ConvexClientWithAuth(
      ffiClient: fakeFfiClient, authProvider: FakeAuthProvider())

    let result = await client.loginFromCache()

    XCTAssertEqual(try result.get(), FakeAuthProvider.CREDENTIALS)
    XCTAssertNotNil(fakeFfiClient.authProvider)
    let token = try await fakeFfiClient.authProvider?.fetchToken(forceRefresh: false)
    XCTAssertEqual(token, "extracted: \(FakeAuthProvider.CREDENTIALS)")
  }

  func testLogoutClearsAuthCallbackOnFfiClient() async throws {
    let fakeFfiClient = FakeMobileConvexClient()
    let client = ConvexMobile.ConvexClientWithAuth(
      ffiClient: fakeFfiClient, authProvider: FakeAuthProvider())

    // Login first to set up auth
    let _ = await client.login()
    XCTAssertNotNil(fakeFfiClient.authProvider)

    await client.logout()

    XCTAssertNil(fakeFfiClient.authProvider)
  }

  func testLoginUpdatesAuthState() async throws {
    let expectation = self.expectation(description: "authState")
    var credentials: String?
    let client = ConvexMobile.ConvexClientWithAuth(
      ffiClient: FakeMobileConvexClient(), authProvider: FakeAuthProvider())

    let cancellationHandle = client.authState.sink(
      receiveValue: { (value: AuthState<String>) in
        if case .authenticated(let creds) = value {
          credentials = creds
          expectation.fulfill()
        }
      })

    let result = await client.login()

    await fulfillment(of: [expectation], timeout: 10)

    XCTAssertEqual(try result.get(), FakeAuthProvider.CREDENTIALS)
    XCTAssertEqual(credentials, FakeAuthProvider.CREDENTIALS)
  }

  func testForceRefreshCallsLoginFromCacheForFreshToken() async throws {
    let fakeFfiClient = FakeMobileConvexClient()
    let fakeAuthProvider = FakeAuthProvider()
    let client = ConvexMobile.ConvexClientWithAuth(
      ffiClient: fakeFfiClient, authProvider: fakeAuthProvider)

    // Initial login sets the auth callback
    let result = await client.login()
    XCTAssertEqual(try result.get(), FakeAuthProvider.CREDENTIALS)

    // loginFromCache is not called during login() (only login is)
    let callCountAfterLogin = fakeAuthProvider.loginFromCacheCallCount

    // Fetch with forceRefresh: true should call through to loginFromCache
    let token = try await fakeFfiClient.authProvider?.fetchToken(forceRefresh: true)
    XCTAssertEqual(token, "extracted: \(FakeAuthProvider.CREDENTIALS)")
    XCTAssertEqual(fakeAuthProvider.loginFromCacheCallCount, callCountAfterLogin + 1)
  }

  func testTokenRefreshUpdatesAuthCallback() async throws {
    let expectation = self.expectation(description: "setAuthCallbackCalled")
    let fakeFfiClient = FakeMobileConvexClient()
    let fakeAuthProvider = FakeAuthProvider()
    let client = ConvexMobile.ConvexClientWithAuth(
      ffiClient: fakeFfiClient, authProvider: fakeAuthProvider)

    // Initial login sets the auth callback
    let result = await client.login()
    XCTAssertEqual(try result.get(), FakeAuthProvider.CREDENTIALS)
    let initialToken = try await fakeFfiClient.authProvider?.fetchToken(forceRefresh: false)
    XCTAssertEqual(initialToken, "extracted: \(FakeAuthProvider.CREDENTIALS)")

    // Set up expectation for the next setAuthCallback call
    fakeFfiClient.setAuthCallbackExpectation = expectation

    // Simulate a token refresh by invoking the stored callback with a new token
    let refreshedToken = "refreshed_token_value"
    fakeAuthProvider.simulateTokenRefresh(newToken: refreshedToken)

    await fulfillment(of: [expectation], timeout: 10)

    // The provider's cached token should now be the refreshed token
    let updatedToken = try await fakeFfiClient.authProvider?.fetchToken(forceRefresh: false)
    XCTAssertEqual(updatedToken, refreshedToken)
  }

  func testTokenRefreshWithNilSetsAuthStateToUnauthenticated() async throws {
    let expectation = self.expectation(description: "authStateUnauthenticated")
    let fakeFfiClient = FakeMobileConvexClient()
    let fakeAuthProvider = FakeAuthProvider()
    let client = ConvexMobile.ConvexClientWithAuth(
      ffiClient: fakeFfiClient, authProvider: fakeAuthProvider)

    var didBecomeUnauthenticated = false
    let cancellationHandle = client.authState.sink(
      receiveValue: { (value: AuthState<String>) in
        if case .unauthenticated = value {
          // Skip the initial unauthenticated state
          if didBecomeUnauthenticated {
            expectation.fulfill()
          }
        } else if case .authenticated = value {
          didBecomeUnauthenticated = true
        }
      })

    // Initial login sets the auth state to authenticated
    let result = await client.login()
    XCTAssertEqual(try result.get(), FakeAuthProvider.CREDENTIALS)

    // Simulate a token refresh with nil (session became invalid)
    fakeAuthProvider.simulateTokenRefresh(newToken: nil)

    await fulfillment(of: [expectation], timeout: 10)

    // Verify the auth callback provider was cleared and state is unauthenticated
    XCTAssertNil(fakeFfiClient.authProvider)
  }

  private func waitUntil(
    timeout: TimeInterval = 2,
    _ condition: @escaping () -> Bool
  ) async -> Bool {
    let deadline = Date().addingTimeInterval(timeout)
    while Date() < deadline {
      if condition() {
        return true
      }
      try? await Task.sleep(nanoseconds: 1_000_000)
    }
    return condition()
  }

  private func publishUntilReceived(
    _ subscriber: any UniFFI.QuerySubscriber,
    receivedValues: LockedValue<Int>
  ) async -> Bool {
    let initialCount = receivedValues.value
    let deadline = Date().addingTimeInterval(2)
    while Date() < deadline {
      subscriber.onUpdate(value: FakeMobileConvexClient.messageJSON)
      if receivedValues.value > initialCount {
        return true
      }
      try? await Task.sleep(nanoseconds: 1_000_000)
    }
    return receivedValues.value > initialCount
  }
}

class FakeMobileConvexClient: UniFFI.MobileConvexClientProtocol {
  static let messageJSON =
    "{\"_id\": \"the_id\", \"val\": {\"$integer\":\"KgAAAAAAAAA=\"}, \"extra\": null}"

  private struct CapturedSubscription {
    let name: String
    let subscriber: any UniFFI.QuerySubscriber
    let handle: FakeSubscriptionHandle
  }

  private let subscriptionLock = NSLock()
  private var capturedSubscriptions: [CapturedSubscription] = []
  private var storedCancellationCount = 0
  private let controlledSubscriptions: Bool
  private let subscriptionResolutionGate: SubscriptionResolutionGate?
  private let updateBeforeSubscriptionReturns: Bool

  var cancellationCount: Int {
    subscriptionLock.withLock { storedCancellationCount }
  }

  var subscriptionCount: Int {
    subscriptionLock.withLock { capturedSubscriptions.count }
  }

  var capturedSubscribers: [any UniFFI.QuerySubscriber] {
    sortedCapturedSubscriptions().map(\.subscriber)
  }

  var subscriptionHandles: [FakeSubscriptionHandle] {
    sortedCapturedSubscriptions().map(\.handle)
  }

  var subscriptionArgs: [String: String] = [:]
  var mutationCalls: [String] = []
  var actionCalls: [String] = []
  var auth: String? = nil
  var authProvider: (any AuthTokenProvider)? = nil
  var resultPublished: XCTestExpectation?
  var setAuthExpectation: XCTestExpectation?
  var setAuthCallbackExpectation: XCTestExpectation?

  init(
    initialAuth: String? = nil,
    resultPublished: XCTestExpectation? = nil,
    controlledSubscriptions: Bool = false,
    subscriptionResolutionGate: SubscriptionResolutionGate? = nil,
    updateBeforeSubscriptionReturns: Bool = false
  ) {
    self.auth = initialAuth
    self.resultPublished = resultPublished
    self.controlledSubscriptions = controlledSubscriptions
    self.subscriptionResolutionGate = subscriptionResolutionGate
    self.updateBeforeSubscriptionReturns = updateBeforeSubscriptionReturns
  }

  fileprivate func recordCancellation() {
    subscriptionLock.withLock { storedCancellationCount += 1 }
  }

  private func sortedCapturedSubscriptions() -> [CapturedSubscription] {
    subscriptionLock.withLock {
      capturedSubscriptions.sorted { $0.name < $1.name }
    }
  }

  func action(name: String, args: [String: String]) async throws -> String {
    actionCalls.append(name)
    if name == "nullResult" {
      return "null"
    }
    if name == "typeMismatch" {
      return "\"just a plain string\""
    }
    let receivedConvexInt = args["anInt"]!
    return "{\"_id\": \"the_id\", \"val\": \(receivedConvexInt), \"extra\": null}"
  }

  func mutation(name: String, args: [String: String]) async throws -> String {
    mutationCalls.append(name)
    if name == "nullResult" {
      return "null"
    }
    if name == "typeMismatch" {
      return "\"just a plain string\""
    }
    let receivedConvexInt = args["anInt"]!
    return "{\"_id\": \"the_id\", \"val\": \(receivedConvexInt), \"extra\": null}"
  }

  func query(name: String, args: [String: String]) async throws -> String {
    return "foo"
  }

  func setAuth(token: String?) async throws {
    auth = token
    setAuthExpectation?.fulfill()
  }

  func setAuthCallback(provider: (any AuthTokenProvider)?) async throws {
    authProvider = provider
    setAuthCallbackExpectation?.fulfill()
  }

  func subscribe(name: String, args: [String: String], subscriber: any UniFFI.QuerySubscriber)
    async throws -> UniFFI.SubscriptionHandle
  {
    subscriptionArgs = args
    let handle = FakeSubscriptionHandle(client: self)
    subscriptionLock.withLock {
      capturedSubscriptions.append(
        CapturedSubscription(name: name, subscriber: subscriber, handle: handle))
    }

    if updateBeforeSubscriptionReturns {
      subscriber.onUpdate(value: Self.messageJSON)
    }

    if let subscriptionResolutionGate {
      await subscriptionResolutionGate.waitUntilResolved()
    }

    if controlledSubscriptions {
      return handle
    }

    let _ = Task {
      try await Task.sleep(nanoseconds: UInt64(0.05 * 1_000_000_000))
      if name == "typeMismatch" {
        subscriber.onUpdate(
          value: "\"just a plain string\"")
      } else if name == "missingVal" {
        subscriber.onUpdate(
          value: "{\"_id\": \"the_id\", \"extra\": null}")
      } else if name == "nullVal" {
        subscriber.onUpdate(
          value: "{\"_id\": \"the_id\", \"val\": null, \"extra\": null}")
      } else {
        subscriber.onUpdate(
          value: "{\"_id\": \"the_id\", \"val\": {\"$integer\":\"KgAAAAAAAAA=\"}, \"extra\": null}")
        if name == "dupeVals" {
          subscriber.onUpdate(
            value:
              "{\"_id\": \"the_id\", \"val\": {\"$integer\":\"KgAAAAAAAAA=\"}, \"extra\": null}")
        }
      }
      resultPublished?.fulfill()

    }
    return handle
  }

}

class FakeAuthProvider: AuthProvider {
  static let CREDENTIALS = "credentials, yo"

  private var storedOnIdToken: (@Sendable (String?) -> Void)?
  var loginFromCacheCallCount = 0

  func loginFromCache(onIdToken: @Sendable @escaping (String?) -> Void) async throws -> String {
    loginFromCacheCallCount += 1
    storedOnIdToken = onIdToken
    onIdToken("extracted: \(FakeAuthProvider.CREDENTIALS)")
    return FakeAuthProvider.CREDENTIALS
  }

  func login(onIdToken: @Sendable @escaping (String?) -> Void) async throws -> String {
    storedOnIdToken = onIdToken
    onIdToken("extracted: \(FakeAuthProvider.CREDENTIALS)")
    return FakeAuthProvider.CREDENTIALS
  }

  func logout() async throws {

  }

  func extractIdToken(from authResult: String) -> String {
    return "extracted: \(authResult)"
  }

  /// Simulates a token refresh by invoking the stored callback with a new token.
  func simulateTokenRefresh(newToken: String?) {
    storedOnIdToken?(newToken)
  }
}

class FakeSubscriptionHandle: UniFFI.SubscriptionHandle {
  private let cancellationLock = NSLock()
  private var storedCancelCount = 0
  private weak var client: FakeMobileConvexClient?

  var cancelCount: Int {
    cancellationLock.withLock { storedCancelCount }
  }

  init(client: FakeMobileConvexClient) {
    self.client = client
    super.init(noPointer: UniFFI.SubscriptionHandle.NoPointer())
  }

  required init(unsafeFromRawPointer pointer: UnsafeMutableRawPointer) {
    fatalError("init(unsafeFromRawPointer:) has not been implemented")
  }

  override func cancel() {
    cancellationLock.withLock { storedCancelCount += 1 }
    client?.recordCancellation()
  }
}

private final class LockedValue<Value>: @unchecked Sendable {
  private let lock = NSLock()
  private var storage: Value

  init(_ value: Value) {
    storage = value
  }

  var value: Value {
    lock.withLock { storage }
  }

  @discardableResult
  func withValue<Result>(_ body: (inout Value) throws -> Result) rethrows -> Result {
    lock.lock()
    defer { lock.unlock() }
    return try body(&storage)
  }
}

final class SubscriptionResolutionGate: @unchecked Sendable {
  private let lock = NSLock()
  private var isResolved = false
  private var continuations: [CheckedContinuation<Void, Never>] = []

  func waitUntilResolved() async {
    await withCheckedContinuation { continuation in
      lock.lock()
      if isResolved {
        lock.unlock()
        continuation.resume()
      } else {
        continuations.append(continuation)
        lock.unlock()
      }
    }
  }

  func resolve() {
    let pendingContinuations: [CheckedContinuation<Void, Never>]
    lock.lock()
    guard !isResolved else {
      lock.unlock()
      return
    }
    isResolved = true
    pendingContinuations = continuations
    continuations.removeAll()
    lock.unlock()

    for continuation in pendingContinuations {
      continuation.resume()
    }
  }
}

private struct MessageWithOptionalVal: Decodable {
  let id: String
  @OptionalConvexInt
  var val: Int? = nil

  enum CodingKeys: String, CodingKey {
    case id = "_id"
    case val
  }
}

private struct Message: Decodable, Equatable {
  let id: String
  @ConvexInt
  var val: Int

  enum CodingKeys: String, CodingKey {
    case id = "_id"
    case val
  }
}
