package es.udc.gac.pardre.mapreduce;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("rawtypes")
public class InMapperCombiner<KEY extends WritableComparable, VALUE extends Writable> {

	public static final Integer CACHE_SIZE = 32768;
	private static final float LOAD_FACTOR = .75F;

	private int maxCacheCapacity;
	private Map<KEY, List<VALUE>> cache;
	private CombiningFunction combiningFunction;

	public InMapperCombiner(CombiningFunction combiningFunction) {
		this.maxCacheCapacity = CACHE_SIZE;
		this.combiningFunction = combiningFunction;
		this.cache = null;
	}

	private Map<KEY, List<VALUE>> getMap() {
		if(cache == null) { //lazy loading
			// Initial capacity is calculated trying to avoid any rehashing operation
			int initialCapacity = (int) Math.ceil((this.maxCacheCapacity+1)/LOAD_FACTOR);
			cache = new LinkedHashMap<KEY, List<VALUE>>(initialCapacity, LOAD_FACTOR);
			System.out.println("InMapperCombiner cache size = "+this.maxCacheCapacity);
		}
		return cache;
	}

	public void setMaxCacheCapacity(int maxCacheCapacity) {
		this.maxCacheCapacity = maxCacheCapacity;
	}

	public void setCombiningFunction(CombiningFunction combiningFunction) {
		this.combiningFunction = combiningFunction;
	}

	@SuppressWarnings("unchecked")
	public void write(KEY key, VALUE value, Context context) throws IOException, InterruptedException {

		if (combiningFunction != null) {
			Map<KEY, List<VALUE>> cache = getMap();
			org.apache.hadoop.conf.Configuration conf = context.getConfiguration();
			key = WritableUtils.clone(key, conf);
			value = WritableUtils.clone(value, conf);

			if (!cache.containsKey(key)) {
				List<VALUE> values = new ArrayList<VALUE>();
				values.add(value);
				cache.put(key, values);
			} else {
				combiningFunction.combine(cache, cache.get(key), value);
			}
		} else {
			context.write(key, value);
		}
	}

	@SuppressWarnings("unchecked")
	public void flush(Context context, boolean force) throws IOException, InterruptedException {

		if (cache == null || (!force && cache.size() < maxCacheCapacity))
			return;

		// Emit key-value pairs from the cache
		if (!cache.isEmpty()) {
			for (Map.Entry<KEY, List<VALUE>> entry : cache.entrySet()) {
				List<VALUE> values = entry.getValue();
				for (VALUE value : values) {
					context.write(entry.getKey(), value);
				}
				values.clear();
			}
			cache.clear();
		}
	}
}
