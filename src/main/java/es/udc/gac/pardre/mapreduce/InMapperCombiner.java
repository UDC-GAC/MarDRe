package es.udc.gac.pardre.mapreduce;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("rawtypes")
public class InMapperCombiner<KEY extends WritableComparable, VALUE extends Writable> {

	private static final int DEFAULT_CAPACITY = 65536;
	private static final int DEFAULT_INITIAL_CAPACITY = 4096;
	private static final float DEFAULT_LOAD_FACTOR = .75F;

	private final int maxCacheCapacity;
	private final Map<KEY, List<VALUE>> LRUCache;
	private CombiningFunction combiningFunction;
	private Context context;

	public InMapperCombiner(int cacheCapacity, CombiningFunction combiningFunction, int initialCapacity, float loadFactor) {
		this.combiningFunction = combiningFunction;
		this.maxCacheCapacity = cacheCapacity;
		LRUCache = new LinkedHashMap<KEY, List<VALUE>>(initialCapacity, loadFactor, true) {
			private static final long serialVersionUID = 1L;

			@Override
			@SuppressWarnings("unchecked")
			protected boolean removeEldestEntry(Map.Entry<KEY, List<VALUE>> eldest) {
				boolean isFull = size() > maxCacheCapacity;
				if (isFull) {
					// If the cache is full, emit the eldest key-values entry to the 
					// reducer, and delete them from cache
					System.out.println("Cache is full");
					try {
						for (VALUE value : eldest.getValue()) {
							context.write(eldest.getKey(), value);
						}
					} catch (IOException ex) {
						throw new UncheckedIOException(ex);
					} catch (InterruptedException ex) {
						throw new UncheckedInterruptedException(ex);
					}
				}
				return isFull;
			}
		};
	}

	public InMapperCombiner() {
		this(DEFAULT_CAPACITY, null, DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
	}

	public InMapperCombiner(int cacheCapacity) {
		this(cacheCapacity, null, DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
	}

	public InMapperCombiner(CombiningFunction combiningFunction) {
		this(DEFAULT_CAPACITY, combiningFunction, 512, .75F);
	}

	public InMapperCombiner(int cacheCapacity, CombiningFunction combiningFunction) {
		this(cacheCapacity, combiningFunction, DEFAULT_INITIAL_CAPACITY, .75F);
	}

	public void setCombiningFunction(CombiningFunction combiningFunction) {
		this.combiningFunction = combiningFunction;
	}

	@SuppressWarnings("unchecked")
	public void write(KEY key, VALUE value, Context context) throws IOException, InterruptedException {
		this.context = context;

		if (combiningFunction != null) {
			if (!LRUCache.containsKey(key)) {
				System.out.println("Key "+key+" not cached");
				List<VALUE> values = new ArrayList<VALUE>();
				values.add(value);
				LRUCache.put(key, values);
			} else {
				System.out.println("Key "+key+" cached, try to combine");
				combiningFunction.combine(LRUCache, LRUCache.get(key), value);
			}
		} else {
			context.write(key, value);
		}
	}

	@SuppressWarnings("unchecked")
	public void flush(Context context) throws IOException, InterruptedException {
		// Emit each key-value pair from the LRU cache.
		System.out.println("Flushing "+LRUCache.size()+ " entries");

		if (!LRUCache.isEmpty()) {
			for (Map.Entry<KEY, List<VALUE>> item : LRUCache.entrySet()) {
				for (VALUE value : item.getValue()) {
					context.write(item.getKey(), value);
				}
				item.getValue().clear();
			}
			LRUCache.clear();
		}
	}

	private static class UncheckedIOException extends java.lang.RuntimeException {
		private static final long serialVersionUID = 1L;

		public UncheckedIOException(Throwable throwable) {
			super(throwable);
		}
	}

	private static class UncheckedInterruptedException extends java.lang.RuntimeException {
		private static final long serialVersionUID = 1L;

		public UncheckedInterruptedException(Throwable throwable) {
			super(throwable);
		}
	}
}
