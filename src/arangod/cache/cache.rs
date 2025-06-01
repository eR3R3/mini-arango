use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use parking_lot::RwLock;
use crate::common::error::Result;

/// Node for LRU cache linked list
struct Node<K, V> {
    key: K,
    value: V,
    prev: Option<Arc<RwLock<Node<K, V>>>>,
    next: Option<Arc<RwLock<Node<K, V>>>>,
}

impl<K, V> Node<K, V> {
    fn new(key: K, value: V) -> Self {
        Node {
            key,
            value,
            prev: None,
            next: None,
        }
    }
}

type NodePtr<K, V> = Arc<RwLock<Node<K, V>>>;

/// LRU Cache implementation
pub struct LRUCache<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    capacity: usize,
    map: RwLock<HashMap<K, NodePtr<K, V>>>,
    head: NodePtr<K, V>,
    tail: NodePtr<K, V>,
}

impl<K, V> LRUCache<K, V>
where
    K: Hash + Eq + Clone + Default,
    V: Clone + Default,
{
    pub fn new(capacity: usize) -> Self {
        let head = Arc::new(RwLock::new(Node::new(K::default(), V::default())));
        let tail = Arc::new(RwLock::new(Node::new(K::default(), V::default())));
        
        // Connect head and tail
        head.write().next = Some(Arc::clone(&tail));
        tail.write().prev = Some(Arc::clone(&head));
        
        LRUCache {
            capacity,
            map: RwLock::new(HashMap::new()),
            head,
            tail,
        }
    }
    
    pub fn get(&self, key: &K) -> Option<V> {
        let map = self.map.read();
        if let Some(node) = map.get(key) {
            let value = node.read().value.clone();
            let node_clone = Arc::clone(node);
            drop(map); // Release read lock before move_to_front
            self.move_to_front(node_clone);
            Some(value)
        } else {
            None
        }
    }
    
    pub fn put(&self, key: K, value: V) {
        let mut map = self.map.write();
        
        if let Some(existing_node) = map.get(&key) {
            // Update existing node
            existing_node.write().value = value;
            let node_ptr = Arc::clone(existing_node);
            drop(map); // Release write lock before move_to_front
            self.move_to_front(node_ptr);
        } else {
            // Create new node
            let new_node = Arc::new(RwLock::new(Node::new(key.clone(), value)));
            self.add_node(Arc::clone(&new_node));
            map.insert(key, new_node);
            
            if map.len() > self.capacity {
                // Remove least recently used
                let lru = self.pop_tail();
                if let Some(lru_node) = lru {
                    let lru_key = lru_node.read().key.clone();
                    map.remove(&lru_key);
                }
            }
        }
    }
    
    pub fn remove(&self, key: &K) -> Option<V> {
        let mut map = self.map.write();
        if let Some(node) = map.remove(key) {
            let value = node.read().value.clone();
            drop(map); // Release write lock before remove_node
            self.remove_node(node);
            Some(value)
        } else {
            None
        }
    }
    
    pub fn len(&self) -> usize {
        self.map.read().len()
    }
    
    pub fn is_empty(&self) -> bool {
        self.map.read().is_empty()
    }
    
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    
    pub fn clear(&self) {
        let mut map = self.map.write();
        map.clear();
        
        // Reset head and tail connections
        self.head.write().next = Some(Arc::clone(&self.tail));
        self.tail.write().prev = Some(Arc::clone(&self.head));
    }
    
    fn add_node(&self, node: NodePtr<K, V>) {
        let mut node_guard = node.write();
        let mut head_guard = self.head.write();
        
        node_guard.prev = Some(Arc::clone(&self.head));
        node_guard.next = head_guard.next.clone();
        
        if let Some(next_node) = &head_guard.next {
            next_node.write().prev = Some(Arc::clone(&node));
        }
        
        head_guard.next = Some(Arc::clone(&node));
    }
    
    fn remove_node(&self, node: NodePtr<K, V>) {
        let node_guard = node.read();
        
        if let Some(prev_node) = &node_guard.prev {
            prev_node.write().next = node_guard.next.clone();
        }
        
        if let Some(next_node) = &node_guard.next {
            next_node.write().prev = node_guard.prev.clone();
        }
    }
    
    fn move_to_front(&self, node: NodePtr<K, V>) {
        self.remove_node(Arc::clone(&node));
        self.add_node(node);
    }
    
    fn pop_tail(&self) -> Option<NodePtr<K, V>> {
        let tail_guard = self.tail.read();
        if let Some(last_node) = &tail_guard.prev {
            if Arc::ptr_eq(last_node, &self.head) {
                return None; // Cache is empty
            }
            
            let last_node_clone = Arc::clone(last_node);
            drop(tail_guard); // Release read lock
            self.remove_node(last_node_clone.clone());
            Some(last_node_clone)
        } else {
            None
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub memory_usage: u64,
}

impl CacheStats {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// Cache with statistics tracking
pub struct StatsCache<K, V>
where
    K: Hash + Eq + Clone + Default,
    V: Clone + Default,
{
    cache: LRUCache<K, V>,
    stats: RwLock<CacheStats>,
}

impl<K, V> StatsCache<K, V>
where
    K: Hash + Eq + Clone + Default,
    V: Clone + Default,
{
    pub fn new(capacity: usize) -> Self {
        StatsCache {
            cache: LRUCache::new(capacity),
            stats: RwLock::new(CacheStats::new()),
        }
    }
    
    pub fn get(&self, key: &K) -> Option<V> {
        let result = self.cache.get(key);
        let mut stats = self.stats.write();
        
        if result.is_some() {
            stats.hits += 1;
        } else {
            stats.misses += 1;
        }
        
        result
    }
    
    pub fn put(&self, key: K, value: V) {
        let old_len = self.cache.len();
        self.cache.put(key, value);
        let new_len = self.cache.len();
        
        if old_len >= self.cache.capacity && new_len == self.cache.capacity {
            let mut stats = self.stats.write();
            stats.evictions += 1;
        }
    }
    
    pub fn stats(&self) -> CacheStats {
        self.stats.read().clone()
    }
    
    pub fn reset_stats(&self) {
        *self.stats.write() = CacheStats::new();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_cache_basic() {
        let cache = LRUCache::new(2);
        
        cache.put("key1".to_string(), "value1".to_string());
        cache.put("key2".to_string(), "value2".to_string());
        
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
        assert_eq!(cache.get(&"key2".to_string()), Some("value2".to_string()));
        
        // This should evict key1 since key2 was accessed more recently
        cache.put("key3".to_string(), "value3".to_string());
        
        assert_eq!(cache.get(&"key1".to_string()), None);
        assert_eq!(cache.get(&"key2".to_string()), Some("value2".to_string()));
        assert_eq!(cache.get(&"key3".to_string()), Some("value3".to_string()));
    }

    #[test]
    fn test_stats_cache() {
        let cache = StatsCache::new(2);
        
        // Miss
        assert_eq!(cache.get(&"missing".to_string()), None);
        
        // Put and hit
        cache.put("key1".to_string(), "value1".to_string());
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
        
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hit_rate(), 0.5);
    }
} 