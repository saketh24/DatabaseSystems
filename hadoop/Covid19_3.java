import java.io.IOException;
import java.util.*;
import java.net.*;
import java.io.*;
import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Covid19_3{
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		private static DoubleWritable map_val = null;
		private Text word = new Text();
		private String world_bool;
		// The 4 types declared here should match the types that was declared on the top
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			world_bool = conf.get("world");
			String line = value.toString();
			String[] vals = line.split(",");
			if(vals[0].equals("date")){
				return;
			}
			map_val = new DoubleWritable(Double.parseDouble(vals[2]));
			word.set(vals[1]);
			context.write(word,map_val);
		}
	}
	
	

	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	// The input types of reduce should match the output type of map
	public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable total = new DoubleWritable();
		URI[] files = null;
		BufferedReader reader = null;
		FileSystem fs = null;
		Path getFilePath = null;
		private Text word = new Text();
		public void setup(Context context){
			try{
				files = context.getCacheFiles();
			}catch(Exception e){
				e.printStackTrace();
				return;
			}
		    for (URI file : files){
		    	 String line = "";
		    	 try{
		    	 	 fs = FileSystem.get(context.getConfiguration()); 
                 	 getFilePath = new Path(file.toString()); 
		    	 }catch(Exception e){
		    	 	e.printStackTrace();
		    	 	return;
		    	 }
		    }
		}
		// Notice the that 2nd argument: type of the input value is an Iterable collection of objects 
		//  with the same type declared above/as the type of output value from map
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			String line = "";
			for (DoubleWritable tmp: values) {
				sum += tmp.get();
			}
			String check_country = key.toString();
			try{
				reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
				while((line = reader.readLine()) != null){
					String[] v = line.split(",");
					if(v[1].equals("location")){
						continue;
					}
					if(v.length > 5){
						continue;
					}
					if(v[1].equals(check_country)){
						Double final_val = sum * 1000000 / (Double.parseDouble(v[4]));
						total.set(final_val);
						context.write(key,total);
						break;
					}
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	
	
	public static void main(String[] args)  throws Exception {
		Configuration conf = new Configuration();
		Job myjob = Job.getInstance(conf, "Task 3 Covid 19");
		myjob.addCacheFile(new Path(args[1]).toUri());
		//myjob.addCacheFile(new Path(args[1]).toUri());
		myjob.setJarByClass(Covid19_3.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(DoubleWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[2]));
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
}