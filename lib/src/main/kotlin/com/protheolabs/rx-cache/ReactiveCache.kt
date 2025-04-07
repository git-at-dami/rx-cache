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
    * A map to store the currently active subscribers and their subscriptions for update events.
     */
    private val updateSubscribers = ConcurrentHashMap<Subscriber<in CacheUpdate<K, V>>, CacheUpdateSubscription<K, V>>()

    /**
     * Subscribes the given [Subscriber] to receive [CacheUpdate] events from this cache.
     * @param s The subscriber that wants to receive updates.
     */
    override fun subscribe(s: Subscriber<in CacheUpdate<K, V>>) {
        // Create a new subscription for this subscriber.
        val subscription = CacheUpdateSubscription(s, this)
        // Store the subscription.
        updateSubscribers[s] = subscription
        // Notify the subscriber that the subscription has been established.
        s.onSubscribe(subscription)
    }

    /**
     * Returns a read-only view of the current update subscribers.
     * This is primarily for testing and monitoring.
     */
    internal fun getSubscribers(): Map<Subscriber<in CacheUpdate<K, V>>, CacheUpdateSubscription<K, V>> {
        return updateSubscribers.toMap()
    }

        /**
     * Removes a subscriber from the active update subscribers.
     * This is called when a subscriber cancels their subscription.
     * @param s The subscriber to remove.
     */
    internal fun removeUpdateSubscription(s: Subscriber<in CacheUpdate<K, V>>) {
        updateSubscribers.remove(s)
    }


    /**
     * Represents the subscription between a Publisher (ReactiveCache) and a Subscriber for CacheUpdate events.
     */
    internal class CacheUpdateSubscription<K, V>(
        private val subscriber: Subscriber<in CacheUpdate<K, V>>,
        private val publisher: ReactiveCache<K, V>
    ) : Subscription {
        private val isCancelled = AtomicBoolean(false)

        /**
         * Requests a specific number of additional items from the Publisher.
         * @param n The number of items to request. Must be positive.
         */
        override fun request(n: Long) {
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
}