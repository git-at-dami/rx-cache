# ReactiveCache: A Basic In-Memory Reactive Cache for Kotlin

This library provides a simple in-memory reactive cache implemented using Kotlin and the Reactive Streams API. It allows you to store key-value item pairs and receive asynchronous updates (puts and removes) as a stream of `CacheUpdate` events. It also provides a way to retrieve the current value for a key as a single-shot reactive stream.

## Table of Contents

1.  [Installation](#1-installation)
2.  [Basic Usage](#2-basic-usage)
    * [Creating a `ReactiveCache`](#creating-a-reactivecache)
    * [Putting Items into the Cache](#putting-items-into-the-cache)
    * [Removing Items from the Cache](#removing-items-from-the-cache)
    * [Getting the Current Value as a Stream](#getting-the-current-value-as-a-stream)
    * [Subscribing to Cache Updates](#subscribing-to-cache-updates)
3.  [Understanding `CacheUpdate`](#3-understanding-cacheupdate)
4.  [Reactive Streams Compliance](#4-reactive-streams-compliance)
5.  [Testing](#5-testing)
6.  [Contributing](#6-contributing)

## 1. Installation

Add the following dependency to your `build.gradle.kts` (Kotlin DSL) or `build.gradle` (Groovy):

**Kotlin DSL (`build.gradle.kts`):**
```kotlin
dependencies {
    implementation("org.reactivestreams:reactive-streams:1.0.4") // Or the latest version
}
```


## 2. Basic Usage

### Creating a ReactiveCache:


```kotlin
val cache = ReactiveCache<String, Int>()
val userCache = ReactiveCache<Long, User>() // Assuming you have a User class
```

### Putting Items into the Cache


```kotlin
cache.put("item1", 100)
cache.put("item2", 200)
```

### Removing Items from the Cache


```kotlin
cache.remove("item1")
cache.remove("nonExistentItem") // Subscribers will not be notified
```

### Getting the Current Value as a Stream

```kotlin
    val item2ValuePublisher: Publisher<Int> = cache.get("item2")
    item2ValuePublisher.subscribe(object : Subscriber<Int> {
        override fun onSubscribe(s: Subscription) { s.request(1) }
        override fun onNext(t: Int) { println("Retrieved value: $t") }
        override fun onError(t: Throwable) { t.printStackTrace() }
        override fun onComplete() { println("Get stream completed") }
    })
    val missingValuePublisher: Publisher<Int> = cache.get("missingItem")
    missingValuePublisher.subscribe { println("Missing value stream completed (no onNext)") }
```

### Subscribing to Cache Updates

```kotlin
cache.subscribe(object : Subscriber<CacheUpdate<String, Int>> {
    private lateinit var subscription: Subscription
    override fun onSubscribe(s: Subscription) { subscription = s; subscription.request(Long.MAX_VALUE) }
    override fun onNext(update: CacheUpdate<String, Int>) {
        when (update) {
            is CacheUpdate.Put -> println("Cache updated: Key {update.key}, Value {update.value")
            is CacheUpdate.Remove -> println("Cache removed: Key '${update.key}'")
        }
    }
    override fun onError(t: Throwable) { t.printStackTrace() }
    override fun onComplete() { println("Update stream completed (should not happen in this implementation)") }
})
cache.put("item3", 300)
cache.remove("item2")
```

## 3. Understanding CacheUpdate

```kotlin
CacheUpdate.Put<K, V>(val key: K, val value: V): Item put/updated.
CacheUpdate.Remove<K>(val key: K): Item removed.
```

##  4. Reactive Streams Compliance
* Implements Publisher.
* Provides Subscription on subscribe.
* Handles demand via request(n: Long).
* Emits updates based on demand.
* Supports cancellation via cancel().
* Non-positive demand signals IllegalArgumentException.

## 5. Testing
Includes JUnit and Mockito tests.

## 6. Contributing
Contributions welcome.


