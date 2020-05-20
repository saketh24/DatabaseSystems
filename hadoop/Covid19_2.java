import java.io.IOException;
import java.util.*;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Covid19_2{
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
		private static LongWritable map_val = null;
		private Text word = new Text();
		private String start_date;
		private String end_date;
		// The 4 types declared here should match the types that was declared on the top
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			start_date = conf.get("start_date");
			end_date = conf.get("end_date");
			String line = value.toString();
			String[] vals = line.split(",");
			if(vals[0].equals("date")){
				return;
			}
			SimpleDateFormat input_format = new SimpleDateFormat("yyyy-MM-dd");
			Date date_val = null, start = null, end = null;
			try{
				date_val = input_format.parse(vals[0]);
				start = input_format.parse(start_date);
				end = input_format.parse(end_date);
			}catch(ParseException e){
				e.printStackTrace();
				return;
			}
			if((date_val.after(start) && date_val.before(end)) || date_val.equals(start) || date_val.equals(end)){
				map_val = new LongWritable(Integer.parseInt(vals[3]));
				word.set(vals[1]);
				context.write(word,map_val);
			}
		}
	}
	
	

	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	// The input types of reduce should match the output type of map
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable total = new LongWritable();
		
		// Notice the that 2nd argument: type of the input value is an Iterable collection of objects 
		//  with the same type declared above/as the type of output value from map
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable tmp: values) {
				sum += tmp.get();
			}
			total.set(sum);
			// This write to the final output
			context.write(key, total);
		}
	}
	
	
	public static void main(String[] args)  throws Exception {
		Configuration conf = new Configuration();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date start_d = null, end_d = null;
		Date file_start = null; 
		Date file_end = null;
		try{
			start_d = format.parse(args[1]);
			end_d = format.parse(args[2]);
			file_start = format.parse("2019-12-31");
			file_end = format.parse("2020-04-08");
		}catch (ParseException bob){
			System.out.println("Invalid input date format");
			return;
		}
		if((end_d.before(start_d)) || (start_d.before(file_start))  || (end_d.after(file_end))){
			System.out.println("Invalid Dates Entered");
			return;
		}
		conf.set("start_date", args[1]);
		conf.set("end_date", args[2]);
		Job myjob = Job.getInstance(conf, "Task 2 Covid 19");
		myjob.setJarByClass(Covid19_2.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[3]));
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
}