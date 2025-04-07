import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.junit.jupiter.api.Assertions.assertFalse
import org.reactivestreams.Subscriber

class CacheUpdateSubscriptionRequestTest {

    @Nested
    inner class When_request_is_called_with_positive_demand {
        @Test
        fun `it_should_not_call_onError`() {
            val subscriber = mock<Subscriber<CacheUpdate<String, Int>>>()
            val publisher = ReactiveCache<String, Int>()
            val subscription = ReactiveCache.CacheUpdateSubscription(subscriber, publisher)

            subscription.request(1)
            subscription.request(10)
            subscription.request(Long.MAX_VALUE)

            verify(subscriber, times(0)).onError(org.mockito.kotlin.any())
        }
    }

    @Nested
    inner class When_request_is_called_with_non_positive_demand {
        @Test
        fun `it_should_call_onError_and_remove_the_subscriber`() {
            val cache = ReactiveCache<String, Int>()
            val subscriber = mock<Subscriber<CacheUpdate<String, Int>>>()
            cache.subscribe(subscriber) // Subscribe to ensure it's in the subscribers map
            val subscriptionEntry = cache.getSubscribers().entries.firstOrNull { it.key == subscriber }
            val actualSubscription = subscriptionEntry?.value

            val nonPositiveDemands = arrayOf(0L, -1L, -5L)
            nonPositiveDemands.forEach { demand ->
                actualSubscription?.request(demand)
            }

            verify(subscriber).onError(org.mockito.kotlin.isA<IllegalArgumentException>())
            // Verify that the subscriber is removed after non-positive demand
            assertFalse(cache.getSubscribers().containsKey(subscriber), "Subscriber should be removed after non-positive demand")
        }
    }

    @Nested
    inner class When_request_is_called_after_cancellation {
        @Test
        fun `it_should_not_call_onError`() {
            val subscriber = mock<Subscriber<CacheUpdate<String, Int>>>()
            val publisher = ReactiveCache<String, Int>()
            val subscription = ReactiveCache.CacheUpdateSubscription(subscriber, publisher)
            subscription.cancel()

            subscription.request(0)
            subscription.request(-1)
            subscription.request(1)

            verify(subscriber, times(0)).onError(org.mockito.kotlin.any())
        }
    }
}