import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertNotNull
import org.reactivestreams.Subscription
import org.reactivestreams.Subscriber

class ReactiveCacheDeleteTest {
      @Nested
    inner class When_delete_is_called {
        @Test
        fun `it_should_delete_the_item_from_the_cache_and_notify_subscribers`() {
            val cache = ReactiveCache<String, Int>()
            val subscriber = mock<Subscriber<CacheUpdate<String, Int>>>()
            val keyToDelete = "toBeDeleted"
            val initialValue = 999

            doAnswer {
                val sub = it.arguments[0] as Subscription
                sub.request(1)
            }.`when`(subscriber).onSubscribe(any())

            cache.put(keyToDelete, initialValue)
            assertNotNull(cache.getSnapshot()[keyToDelete], "item added to the cache")

            cache.subscribe(subscriber)

            cache.delete(keyToDelete)

            assertNull(cache.getSnapshot()[keyToDelete], "item should be deleted from the cache")
            verify(subscriber, times(1)).onNext(CacheUpdate.Delete(keyToDelete))
        }

        @Test
        fun `it_should_not_notify_subscribers_if_the_key_does_not_exist`() {
            val cache = ReactiveCache<String, Int>()
            val subscriber = mock<Subscriber<CacheUpdate<String, Int>>>()

            doAnswer {
                val sub = it.arguments[0] as Subscription
                sub.request(1)
            }.`when`(subscriber).onSubscribe(any())

            val nonExistingKey = "notHere"

            cache.delete(nonExistingKey)

            assertNull(cache.getSnapshot()[nonExistingKey], "Key should not be in the cache")
            verify(subscriber, times(0)).onNext(any())
        }
    }
}