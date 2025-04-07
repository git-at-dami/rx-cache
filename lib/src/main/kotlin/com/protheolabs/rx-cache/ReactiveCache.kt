import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * in-memory reactive cache that emits updates through the Reactive Streams API.
 */
class ReactiveCache<K, V> : Publisher<CacheUpdate<K, V>> {
    /**
     * The underlying in-memory cache.
     */
    private val cache = ConcurrentHashMap<K, V>()

    /**
    * A map to store the currently active subscribers and their subscriptions for update events.
     */
    private val subscribers = ConcurrentHashMap<Subscriber<in CacheUpdate<K, V>>, CacheUpdateSubscription<K, V>>()

    /**
     * Subscribes the given [Subscriber] to receive [CacheUpdate] events from this cache.
     * @param s The subscriber that wants to receive updates.
     */
    override fun subscribe(s: Subscriber<in CacheUpdate<K, V>>) {
        // Create a new subscription for this subscriber.
        val subscription = CacheUpdateSubscription(s, this)
        // Store the subscription.
        subscribers[s] = subscription
        // Notify the subscriber that the subscription has been established.
        s.onSubscribe(subscription)
    }

    /**
     * Returns a read-only view of the current update subscribers.
     * This is primarily for testing and monitoring.
     */
    internal fun getSubscribers(): Map<Subscriber<in CacheUpdate<K, V>>, CacheUpdateSubscription<K, V>> {
        return subscribers.toMap()
    }

        /**
     * Removes a subscriber from the active update subscribers.
     * This is called when a subscriber cancels their subscription.
     * @param s The subscriber to remove.
     */
    internal fun removeUpdateSubscription(s: Subscriber<in CacheUpdate<K, V>>) {
        subscribers.remove(s)
    }


    /**
     * Represents the subscription between a Publisher (ReactiveCache) and a Subscriber for CacheUpdate events.
     */
    internal class CacheUpdateSubscription<K, V>(
        private val subscriber: Subscriber<in CacheUpdate<K, V>>,
        private val publisher: ReactiveCache<K, V>
    ) : Subscription {
        private val isCancelled = AtomicBoolean(false)
        private val requested = AtomicLong(0)

        /**
         * Requests a specific number of additional items from the Publisher.
         * @param n The number of items to request. Must be positive. Subscription gets cancelled when n is negative
         */
        override fun request(n: Long) {
            if (isCancelled.get()) return
            if (n <= 0) {
                subscriber.onError(IllegalArgumentException("Demand must be positive"))
                cancel()
                return
            }
            requested.getAndAdd(n)
        }

         /**
         * Called by the Publisher to send the next item to the Subscriber.
         * @param update The cache update event.
         */
        fun onNext(update: CacheUpdate<K, V>) {
            if (!isCancelled.get()) {
                val currentRequested = requested.get()
                if (currentRequested > 0) {
                    requested.decrementAndGet()
                    subscriber.onNext(update)
                } else {
                    // No current demand, we might need to buffer or drop the update.
                }
            }
        }

        /**
         * Cancels the subscription. The Publisher will eventually stop sending more items.
         */
        override fun cancel() {
            if (isCancelled.compareAndSet(false, true)) {
                publisher.removeUpdateSubscription(subscriber)
            }
        }
    }


    /**
     * Puts a key-value pair into the cache and notifies all active update subscribers.
     * @param key The key to put.
     * @param value The value to associate with the key.
     */
    fun put(key: K, value: V) {
        cache[key] = value
        subscribers.forEach { (_, subscription) ->
            subscription.onNext(CacheUpdate.Put(key, value))
        }
    }

    /**
     * Deletes an Item from the cache and notifies all active update subscribers only if the key was present.
     * @param key The key to remove.
     */
    fun delete(key: K) {
        if (cache.containsKey(key)) {
            cache.remove(key)
            subscribers.forEach { (_, subscription) ->
                println("HEYY")
                subscription.onNext(CacheUpdate.Delete(key))
            }
        }
    }

    /**
     * Returns a snapshot of the current cache contents.
     * @return An immutable map representing the current state of the cache.
     */
    fun getSnapshot(): Map<K, V> = cache.toMap()
}