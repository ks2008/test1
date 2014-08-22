package testprograms;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public  class TopTenUserReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private  Map<Text, IntWritable> userCountMap = new HashMap<>();
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
						throws IOException, InterruptedException {
		int totalCount = 0;
		for (IntWritable val : values) {
			totalCount += val.get();
		}
		userCountMap.put(new Text(key), new IntWritable(totalCount));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException {
		
		Map<Text, IntWritable> descendSortedMap = descendSortByValues(userCountMap);
		int resultset = 0;
		for (Text key: descendSortedMap.keySet()) {
			if ( resultset++ == 10) {
				break;
			}
		    context.write(key, descendSortedMap.get(key));
		}
	}

	private <K extends Comparable, V extends Comparable> Map<K,V> descendSortByValues(Map<K,V> map) {
	   List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K,V>>(map.entrySet());
	   
	   Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {
		   @Override 
		   public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
			   return o2.getValue().compareTo(o1.getValue()); // to sort descending by values
		   }
	   });
	   
	   // put entries back in the same order
	   Map<K,V> descendedMap = new LinkedHashMap<K,V>();
	   for (Map.Entry<K, V> entry : entries) {
		   descendedMap.put(entry.getKey(),entry.getValue());
	   }
	   
	   return descendedMap;
	}
}
