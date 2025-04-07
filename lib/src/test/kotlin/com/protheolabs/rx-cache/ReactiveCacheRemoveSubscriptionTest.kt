import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.assertFalse
import org.reactivestreams.Subscriber

class ReactiveCacheRemoveSubscriptionTest {

    @Nested
    inner class When_removeUpdateSubscription_is_called {
        @Test
        fun `it_should_remove_the_subscriber_from_the_updateSubscribers_map`() {
            val cache = ReactiveCache<String, Int>()
            val subscriber: Subscriber<CacheUpdate<String, Int>> = mock<Subscriber<CacheUpdate<String, Int>>>()

            cache.subscribe(subscriber)

            // Get the subscriber key
            val subscriberKey = cache.getSubscribers().keys.firstOrNull()

            assertTrue(cache.getSubscribers().containsKey(subscriberKey), "Subscriber should be initially present")

            cache.removeUpdateSubscription(subscriberKey!!)

            assertFalse(cache.getSubscribers().containsKey(subscriberKey), "Subscriber should be removed after calling removeUpdateSubscription")
        }
    }

    @Nested
    inner class When_cancel_is_called {
        @Test
        fun `it_should_remove_the_subscriber_from_updateSubscribers`() {
            val cache = ReactiveCache<String, Int>()
            val subscriber = mock<org.reactivestreams.Subscriber<CacheUpdate<String, Int>>>()

            cache.subscribe(subscriber)

            // Get the initial number of subscribers
            val initialSubscriberCount = cache.getSubscribers().size
            val subscriberKey = cache.getSubscribers().keys.firstOrNull()
            val actualSubscription = cache.getSubscribers()[subscriberKey]

            actualSubscription?.cancel()

            val finalSubscriberCount = cache.getSubscribers().size

            assertTrue(finalSubscriberCount < initialSubscriberCount)
            assertFalse(cache.getSubscribers().containsKey(subscriberKey), "Subscriber should no longer be in updateSubscribers")
        }
    }
}