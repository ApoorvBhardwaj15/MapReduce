package s3757978.mapReduce.Assignment_2;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class Task_1_1_strips {

	//mapper class
	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, MapWritable> {

		// initiating logger 
		private static final Logger LOG = Logger.getLogger(TokenizerMapper.class);

		//declaring Text and MapWritable variables
		private MapWritable valueMap = new MapWritable();
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);

		
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			LOG.setLevel(Level.DEBUG);
			LOG.debug("Mapper Task of APOORV BHARDWAJ, s3757978");

			// splitting the text based on "\\s+" as delimiter
			String[] sentences = value.toString().split("\n");
			for (int i = 0 ; i < sentences.length ; i++) {

				int temp = sentences[i].length();

				if(temp > 0) {
					// splitting the text based on " " as delimiter
					String[] sentence = sentences[i].split(" ");
					for (int j = 0 ; j < sentence.length ; j++ ) {
						valueMap.clear();
						
						String cleanTemp = sentence[j].replaceAll("\\W+",""); 
						// removing digits and converting to lower case
						cleanTemp = cleanTemp.replaceAll("\\d+","").toLowerCase();
						if(cleanTemp.length() > 1) {
							word.set(cleanTemp);

							for (int k = 0 ; k< sentence.length; k ++) {
								// skipping identical text
								if(j == k) {
									continue;
								}
								String cleanTemp2 = sentence[k].replaceAll("\\W+","");
								cleanTemp2 = cleanTemp2.replaceAll("\\d+","").toLowerCase();
								if(cleanTemp2.length() > 1) {	//ensuring length is >1
									Text contextWord = new Text(cleanTemp2);
									if (valueMap.containsKey(contextWord)) {
										IntWritable count = (IntWritable) valueMap.get(contextWord); 
										// assigning value to map
										valueMap.put(contextWord,new IntWritable(count.get()+1));
									}
									else {
										valueMap.put(contextWord, one);
									}
								}
							}
						}
						// writing to hdfs
						context.write(word, valueMap);
					}
				}

			}
		}
	}

	//reducer class
	public static class IntSumReducer
	extends Reducer<Text,MapWritable,Text,MapWritable> {

		// initate logger
		private static final Logger LOG = Logger.getLogger(IntSumReducer.class);

		private MapWritable addedMap = new MapWritable();

		public void reduce(Text key, Iterable<MapWritable> values,
				Context context
				) throws IOException, InterruptedException {

			LOG.setLevel(Level.DEBUG);
			LOG.debug("Reducer Task of APOORV BHARDWAJ, s3757978");

			addedMap.clear();
			for(MapWritable value : values) {
				// addAll is a custom method defined below
				addAll(value);
			}
			
			//writing to hdfs
			context.write(key, addedMap);


		}
		
		// adding all the maps belongs to a key
		private void addAll(MapWritable mapWritable) {
			// TODO Auto-generated method stub
			Set<Writable> keys = mapWritable.keySet();
			for (Writable key : keys) {
				IntWritable newCount = (IntWritable) mapWritable.get(key);
				if (addedMap.containsKey(key)) {
					IntWritable existingCount = (IntWritable) addedMap.get(key);				
					addedMap.put(key, new IntWritable(existingCount.get() + newCount.get()));
				} else {
					addedMap.put(key, newCount);
				}
			}
		}
	}

	// driver class
	public static void main(String[] args) throws Exception {

		// basic configuration
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Task_1_1_strips");
		job.setJarByClass(Task_1_1_strips.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}
