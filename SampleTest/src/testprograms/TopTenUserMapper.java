package testprograms;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TopTenUserMapper extends Mapper<Object,Text,Text,IntWritable> {
	private Map<String, Integer> usercountMap = new HashMap<>();
		
	@Override
	public void map(Object key, Text value, Context context)
	  throws IOException, InterruptedException {
		
		String[] logitems = value.toString().split("&");
		
		String eventdate = logitems[0].substring(0,10);
	    String userid =  logitems[1];
	    String eventid = logitems[13];
		
	    if (eventdate.equals("2012-07-17")){  // for a given date
			if (eventid.equals("WT.ev=ApplicationLaunch")) { // for a given event
				if (usercountMap.containsKey(userid)) {
				   usercountMap.put(userid,usercountMap.get(userid)+1);
				} else {
					usercountMap.put(userid,1);
				}
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

