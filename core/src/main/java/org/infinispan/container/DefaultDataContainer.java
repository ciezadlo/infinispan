package org.infinispan.container;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.Immutables;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap.Eviction;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap.EvictionListener;

/**
 * Simple data container that does not order entries for eviction, implemented using two ConcurrentHashMaps, one for
 * mortal and one for immortal entries.
 * <p/>
 * This container does not support eviction, in that entries are unsorted.
 * <p/>
 * This implementation offers O(1) performance for all operations.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@ThreadSafe
public class DefaultDataContainer implements DataContainer {
   final ConcurrentMap<Object, InternalCacheEntry> immortalEntries;
   final ConcurrentMap<Object, InternalCacheEntry> mortalEntries;
   final AtomicInteger numEntries = new AtomicInteger(0);
   final InternalEntryFactory entryFactory;
   final DefaultEvictionListener evictionListener; 
   protected Cache<Object, Object> cache;


   protected DefaultDataContainer(int concurrencyLevel) {
      this(concurrencyLevel, false, false);
   }

   protected DefaultDataContainer(int concurrencyLevel, boolean recordCreation, boolean recordLastUsed) {
      immortalEntries = new ConcurrentHashMap<Object, InternalCacheEntry>(128, 0.75f, concurrencyLevel);
      mortalEntries = new ConcurrentHashMap<Object, InternalCacheEntry>(64, 0.75f, concurrencyLevel);
      entryFactory = new InternalEntryFactory(recordCreation, recordLastUsed);
      evictionListener = null;
   }
   
   protected DefaultDataContainer(int concurrencyLevel, int maxEntries, EvictionStrategy strategy, EvictionThreadPolicy policy,
            boolean recordCreation, boolean recordLastUsed) {
      
      // translate eviction policy and strategy
      switch (policy) {
         case DEFAULT:
            evictionListener = new DefaultEvictionListener();
            break;
         case PIGGYBACK:
            evictionListener = new PiggybackEvictionListener();
            break;
         default:
            throw new IllegalArgumentException("No such eviction thread policy " + strategy);
      }
      
      Eviction eviction;
      switch (strategy) {
         case FIFO:
         case LRU:
            eviction = Eviction.LRU;            
            break;
         case LIRS:
            eviction = Eviction.LIRS;            
            break;
         default:
            throw new IllegalArgumentException("No such eviction strategy " + strategy);
      }
      immortalEntries = new BoundedConcurrentHashMap<Object, InternalCacheEntry>(maxEntries, concurrencyLevel, eviction, evictionListener);
      mortalEntries = new ConcurrentHashMap<Object, InternalCacheEntry>(64, 0.75f, concurrencyLevel);
      entryFactory = new InternalEntryFactory(recordCreation, recordLastUsed);
   }
   
   @Inject
   public void initialize(Cache<Object, Object> cache) {      
      this.cache = cache;    
   }
   
   public static DataContainer boundedDataContainer(int concurrencyLevel, int maxEntries, EvictionStrategy strategy, EvictionThreadPolicy policy) {
      return new DefaultDataContainer(concurrencyLevel, maxEntries, strategy,policy, false, false) {

         @Override
         public int size() {
            return immortalEntries.size() + mortalEntries.size();
         }

         @Override
         public Set<InternalCacheEntry> getEvictionCandidates() {
            return evictionListener.getEvicted();
         }
      };
   }
   
   public static DataContainer unBoundedDataContainer(int concurrencyLevel) {
      return new DefaultDataContainer(concurrencyLevel) ;
   }
   
   @Override
   public Set<InternalCacheEntry> getEvictionCandidates() {
      return Collections.emptySet();
   }

   public InternalCacheEntry peek(Object key) {
      InternalCacheEntry e = immortalEntries.get(key);
      if (e == null) e = mortalEntries.get(key);
      return e;
   }

   public InternalCacheEntry get(Object k) {
      InternalCacheEntry e = peek(k);
      if (e != null) {
         if (e.isExpired()) {
            mortalEntries.remove(k);
            numEntries.getAndDecrement();
            e = null;
         } else {
            e.touch();
         }
      }
      return e;
   }

   protected void successfulPut(InternalCacheEntry ice, boolean newEntry) {
      // no-op
   }

   public void put(Object k, Object v, long lifespan, long maxIdle) {
      InternalCacheEntry e = immortalEntries.get(k);
      if (e != null) {
         e.setValue(v);
         e = entryFactory.update(e, lifespan, maxIdle);

         if (e.canExpire()) {
            immortalEntries.remove(k);
            mortalEntries.put(k, e);
         }
         successfulPut(e, false);
      } else {
         e = mortalEntries.get(k);
         if (e != null) {
            e.setValue(v);
            InternalCacheEntry original = e;
            e = entryFactory.update(e, lifespan, maxIdle);

            if (!e.canExpire()) {
               mortalEntries.remove(k);
               immortalEntries.put(k, e);
            } else if (e != original) {
               // the entry has changed type, but still can expire!
               mortalEntries.put(k, e);
            }
            successfulPut(e, false);
         } else {
            // this is a brand-new entry
            numEntries.getAndIncrement();
            e = entryFactory.createNewEntry(k, v, lifespan, maxIdle);
            if (e.canExpire())
               mortalEntries.put(k, e);
            else
               immortalEntries.put(k, e);
            successfulPut(e, true);
         }
      }
   }

   public boolean containsKey(Object k) {
      InternalCacheEntry ice = peek(k);
      if (ice != null && ice.isExpired()) {
         mortalEntries.remove(k);
         numEntries.getAndDecrement();
         ice = null;
      }
      return ice != null;
   }

   public InternalCacheEntry remove(Object k) {
      InternalCacheEntry e = immortalEntries.remove(k);
      if (e == null) e = mortalEntries.remove(k);
      if (e != null) numEntries.getAndDecrement();

      return e == null || e.isExpired() ? null : e;
   }

   public int size() {
      return numEntries.get();
   }

   public void clear() {
      immortalEntries.clear();
      mortalEntries.clear();
      numEntries.set(0);
   }

   public Set<Object> keySet() {
      return new KeySet();
   }

   public Collection<Object> values() {
      return new Values();
   }

   public Set<InternalCacheEntry> entrySet() {
      return new EntrySet();
   }

   public void purgeExpired() {
      for (Iterator<InternalCacheEntry> entries = mortalEntries.values().iterator(); entries.hasNext();) {
         InternalCacheEntry e = entries.next();
         if (e.isExpired()) {
            entries.remove();
            numEntries.getAndDecrement();
         }
      }
   }

   public Iterator<InternalCacheEntry> iterator() {
      return new EntryIterator(immortalEntries.values().iterator(), mortalEntries.values().iterator());
   }
   
   private class DefaultEvictionListener implements EvictionListener<Object, InternalCacheEntry>{
      final List <InternalCacheEntry> evicted = Collections.synchronizedList(new LinkedList<InternalCacheEntry>());

      @Override
      public void evicted(Object key, InternalCacheEntry value) {
         evicted.add(value);
      }   
      
      public Set<InternalCacheEntry> getEvicted() {
         synchronized (evicted) {
            return new HashSet<InternalCacheEntry>(evicted);
         } 
      }
   }
   
   private class PiggybackEvictionListener extends  DefaultEvictionListener{
      
      @Override
      public void evicted(Object key, InternalCacheEntry value) {
         cache.getAdvancedCache().evict(key);
      }  
      
      public Set<InternalCacheEntry> getEvicted() {
         return Collections.emptySet();
      }
   }

   private class KeySet extends AbstractSet<Object> {
      final Set<Object> immortalKeys;
      final Set<Object> mortalKeys;

      public KeySet() {
         immortalKeys = immortalEntries.keySet();
         mortalKeys = mortalEntries.keySet();
      }

      public Iterator<Object> iterator() {
         return new KeyIterator(immortalKeys.iterator(), mortalKeys.iterator());
      }

      public void clear() {
         throw new UnsupportedOperationException();
      }

      public boolean contains(Object o) {
         return immortalKeys.contains(o) || mortalKeys.contains(o);
      }

      public boolean remove(Object o) {
         throw new UnsupportedOperationException();
      }

      public int size() {
         return immortalKeys.size() + mortalKeys.size();
      }
   }

   private static class KeyIterator implements Iterator<Object> {
      Iterator<Iterator<Object>> metaIterator;
      Iterator<Object> currentIterator;

      private KeyIterator(Iterator<Object> immortalIterator, Iterator<Object> mortalIterator) {
         metaIterator = Arrays.asList(immortalIterator, mortalIterator).iterator();
         if (metaIterator.hasNext()) currentIterator = metaIterator.next();
      }

      public boolean hasNext() {
         boolean hasNext = currentIterator.hasNext();
         while (!hasNext && metaIterator.hasNext()) {
            currentIterator = metaIterator.next();
            hasNext = currentIterator.hasNext();
         }
         return hasNext;
      }

      @SuppressWarnings("unchecked")
      public Object next() {
         return currentIterator.next();
      }

      public void remove() {
         throw new UnsupportedOperationException();
      }
   }

   private class EntrySet extends AbstractSet<InternalCacheEntry> {
      public Iterator<InternalCacheEntry> iterator() {
         return new ImmutableEntryIterator(immortalEntries.values().iterator(), mortalEntries.values().iterator());
      }

      @Override
      public int size() {
         return immortalEntries.size() + mortalEntries.size();
      }
   }

   private static class MortalInmortalIterator {
      Iterator<Iterator<InternalCacheEntry>> metaIterator;
      Iterator<InternalCacheEntry> currentIterator;

      private MortalInmortalIterator(Iterator<InternalCacheEntry> immortalIterator, Iterator<InternalCacheEntry> mortalIterator) {
         metaIterator = Arrays.asList(immortalIterator, mortalIterator).iterator();
         if (metaIterator.hasNext()) currentIterator = metaIterator.next();
      }

      public boolean hasNext() {
         boolean hasNext = currentIterator.hasNext();
         while (!hasNext && metaIterator.hasNext()) {
            currentIterator = metaIterator.next();
            hasNext = currentIterator.hasNext();
         }
         return hasNext;
      }

      public void remove() {
         throw new UnsupportedOperationException();
      }
   }

   private class EntryIterator extends MortalInmortalIterator implements Iterator<InternalCacheEntry> {
      private EntryIterator(Iterator<InternalCacheEntry> immortalIterator, Iterator<InternalCacheEntry> mortalIterator) {
         super(immortalIterator, mortalIterator);
      }

      @SuppressWarnings("unchecked")
      public InternalCacheEntry next() {
         return currentIterator.next();
      }
   }

   private class ImmutableEntryIterator extends MortalInmortalIterator implements Iterator<InternalCacheEntry> {
      private ImmutableEntryIterator(Iterator<InternalCacheEntry> immortalIterator, Iterator<InternalCacheEntry> mortalIterator) {
         super(immortalIterator, mortalIterator);
      }

      public InternalCacheEntry next() {
         return Immutables.immutableInternalCacheEntry(currentIterator.next());
      }
   }

   private class Values extends AbstractCollection<Object> {
      @Override
      public Iterator<Object> iterator() {
         return new ValueIterator(immortalEntries.values().iterator(), mortalEntries.values().iterator());
      }

      @Override
      public int size() {
         return immortalEntries.size() + mortalEntries.size();
      }
   }

   private class ValueIterator extends MortalInmortalIterator implements Iterator<Object> {
      private ValueIterator(Iterator<InternalCacheEntry> immortalIterator, Iterator<InternalCacheEntry> mortalIterator) {
         super(immortalIterator, mortalIterator);
      }

      public Object next() {
         return currentIterator.next().getValue();
      }
   }
}