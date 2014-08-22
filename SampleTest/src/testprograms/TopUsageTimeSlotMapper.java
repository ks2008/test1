package testprograms;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public  class TopUsageTimeSlotMapper extends Mapper<Object,Text,Text,IntWritable> {
	private Map<String, Integer> usercountMap = new HashMap<>();
		
	@Override
	public void map(Object key, Text value, Context context)
	  throws IOException, InterruptedException {
		String[] logitems = value.toString().split("&");
		String peakhour = "21-23";
		int eventhour = new Integer(logitems[0].substring(11,13)).intValue();
		if (eventhour >=9 && eventhour<12) { peakhour = "9-12"; }
		if (eventhour >=12 && eventhour<17) {peakhour = "12-5"; }
		if (eventhour >=18 && eventhour<21) {peakhour = "6-9";  }
		//String userid =  values[1];
	    String eventid = logitems[13];
		
	   	if (eventid.equals("WT.ev=ApplicationLaunch")) {
	   		 if (usercountMap.containsKey(peakhour)) {
			    usercountMap.put(peakhour,usercountMap.get(peakhour)+1);
			 } else {
					usercountMap.put(peakhour,1);
			   }
		 }
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException {
		for (String key: usercountMap.keySet()) {
			context.write(new Text(key),new IntWritable(usercountMap.get(key)));
			
		}
	}
}


