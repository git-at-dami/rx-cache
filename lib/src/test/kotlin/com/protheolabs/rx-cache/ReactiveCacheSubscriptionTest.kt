import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.junit.jupiter.api.Assertions.assertTrue

class ReactiveCacheSubscriptionTest {

    @Nested
    inner class When_a_subscriber_subscribes {
        @Test
        fun `it_should_call_onSubscribe_with_an_instance_of_CacheUpdateSubscription`() {
            val cache = ReactiveCache<String, Int>()
            val subscriber = mock<org.reactivestreams.Subscriber<CacheUpdate<String, Int>>>()

            cache.subscribe(subscriber)

            val capturedSubscription = org.mockito.kotlin.argumentCaptor<org.reactivestreams.Subscription>()
            verify(subscriber).onSubscribe(capturedSubscription.capture())
            assertTrue(capturedSubscription.firstValue is ReactiveCache.CacheUpdateSubscription<*, *>, "Expected a CacheUpdateSubscription")
        }
    }
}