import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.reactivestreams.Subscription
import org.reactivestreams.Subscriber

class ReactiveCachePutTest {

    @Nested
    inner class When_put_is_called {
        @Test
        fun `it_should_store_the_item_in_the_cache_and_notify_subscribers`() {
            val cache = ReactiveCache<String, Int>()
            val subscriber = mock<org.reactivestreams.Subscriber<CacheUpdate<String, Int>>>()
            val key = "testKey"
            val value = 123

            doAnswer {
                val sub = it.arguments[0] as Subscription
                sub.request(1)
            }.`when`(subscriber).onSubscribe(any())

            cache.subscribe(subscriber)

            cache.put(key, value)

            assertEquals(value, cache.getSnapshot()[key], "Value should be stored in the cache")
            verify(subscriber, times(1)).onNext(CacheUpdate.Put(key, value))
        }

        @Test
        fun `it_should_update_the_value_if_the_key_already_exists_and_notify_subscribers`() {
            val cache = ReactiveCache<String, Int>()
            val subscriber = mock<org.reactivestreams.Subscriber<CacheUpdate<String, Int>>>()
            val key = "existingKey"
            val initialValue = 456
            val updatedValue = 789

            doAnswer {
                val sub = it.arguments[0] as Subscription
                sub.request(2)
            }.`when`(subscriber).onSubscribe(any())

            cache.put(key, initialValue)
            cache.subscribe(subscriber) // Subscribe after initial put

            cache.put(key, updatedValue)

            assertEquals(updatedValue, cache.getSnapshot()[key], "Value should be updated in the cache")
            verify(subscriber, times(1)).onNext(CacheUpdate.Put(key, updatedValue))
        }
    }
}