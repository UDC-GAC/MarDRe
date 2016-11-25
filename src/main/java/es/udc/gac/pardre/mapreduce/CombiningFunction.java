package es.udc.gac.pardre.mapreduce;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public interface CombiningFunction<KEY extends WritableComparable, VALUE extends Writable> {
	public void combine(Map<KEY, List<VALUE>> cache, List<VALUE> valuesInCache, VALUE newValue);
}
