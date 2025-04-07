/**
 * Represents an update operation on the cache.
 */
 sealed class CacheUpdate<K, V> {
    /**
     * Represents storing a key-value item pair into the cache.
     * @param key The key of the item
     * @param value The value being associated with the key.
     */
    data class Put<K, V>(val key: K, val value: V) : CacheUpdate<K, V>()

        /**
     * Represents deleting an item from the cache.
     * @param key The key of the item being deleted
     */
    data class Delete<K, V>(val key: K) : CacheUpdate<K, V>()
 }