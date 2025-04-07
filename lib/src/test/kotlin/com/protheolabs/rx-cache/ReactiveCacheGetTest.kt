import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.any
import org.mockito.ArgumentCaptor
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.never
import org.mockito.kotlin.isA
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.util.concurrent.atomic.AtomicReference

class ReactiveCacheGetTest {

    @Test
    fun `get should emit the current value if key exists and demand is present`() {
        val cache = ReactiveCache<String, Int>()
        val key = "existingKey"
        val value = 987
        cache.put(key, value)
        val subscriber = mock<Subscriber<Int>>()
        val emittedValue = AtomicReference<Int>()

        doAnswer {
            val sub = it.arguments[0] as Subscription
            sub.request(1)
        }.`when`(subscriber).onSubscribe(any())

        doAnswer {
            emittedValue.set(it.arguments[0] as Int)
        }.`when`(subscriber).onNext(any())

        cache.get(key).subscribe(subscriber)

        verify(subscriber, times(1)).onNext(value)
        verify(subscriber, times(1)).onComplete()
    }

    @Test
    fun `get should emit onComplete immediately if key does not exist and demand is present`() {
        val cache = ReactiveCache<String, Int>()
        val nonExistingKey = "missingKey"
        val subscriber = mock<Subscriber<Int>>()
        val subscription = mock<Subscription>()

        doAnswer {
            val sub = it.arguments[0] as Subscription
            sub.request(1)
        }.`when`(subscriber).onSubscribe(any())

        cache.get(nonExistingKey).subscribe(subscriber)

        verify(subscriber, never()).onNext(any())
        verify(subscriber, times(1)).onComplete()
    }

    @Test
    fun `get should not emit if no initial demand`() {
        val cache = ReactiveCache<String, Int>()
        val key = "testKey"
        val value = 555
        cache.put(key, value)
        val subscriber = mock<Subscriber<Int>>()

        cache.get(key).subscribe(subscriber)

        verify(subscriber, never()).onNext(any())
        verify(subscriber, never()).onComplete()
    }

    @Test
    fun `get should handle multiple requests but emit only once`() {
        val cache = ReactiveCache<String, Int>()
        val key = "multiRequestKey"
        val value = 111
        cache.put(key, value)
        val subscriber = mock<Subscriber<Int>>()
        val subscription = mock<Subscription>()

        doAnswer {
            val sub = it.arguments[0] as Subscription
            sub.request(1)
            sub.request(2) // Second request
        }.`when`(subscriber).onSubscribe(any())

        cache.get(key).subscribe(subscriber)

        verify(subscriber, times(1)).onNext(value)
        verify(subscriber, times(1)).onComplete()
    }

    @Test
    fun `get should not emit after cancellation`() {
        val cache = ReactiveCache<String, Int>()
        val key = "cancelledKey"
        val value = 777
        cache.put(key, value)
        val subscriber = mock<Subscriber<Int>>()
        val subscriptionCaptor: ArgumentCaptor<Subscription> = ArgumentCaptor.forClass(Subscription::class.java)

        cache.get(key).subscribe(subscriber)
        verify(subscriber).onSubscribe(subscriptionCaptor.capture())

        // Cancel BEFORE requesting
        subscriptionCaptor.value.cancel()
        subscriptionCaptor.value.request(1)

        verify(subscriber, never()).onNext(any())
        verify(subscriber, never()).onComplete()
        verify(subscriber, never()).onError(any())
    }

    @Test
    fun `get should call onError for non-positive demand`() {
        val cache = ReactiveCache<String, Int>()
        val key = "errorKey"
        val subscriber = mock<Subscriber<Int>>()

        doAnswer {
            val sub = it.arguments[0] as Subscription
            sub.request(0)
        }.`when`(subscriber).onSubscribe(any())

        cache.get(key).subscribe(subscriber)

        verify(subscriber, times(1)).onError(isA<IllegalArgumentException>())
        verify(subscriber, never()).onNext(any())
        verify(subscriber, never()).onComplete()

        val subscriber2 = mock<Subscriber<Int>>()
        doAnswer {
            val sub = it.arguments[0] as Subscription
            sub.request(-1)
        }.`when`(subscriber2).onSubscribe(any())
        cache.get(key).subscribe(subscriber2)
        verify(subscriber2, times(1)).onError(isA<IllegalArgumentException>())
        verify(subscriber2, never()).onNext(any())
        verify(subscriber2, never()).onComplete()
    }
}