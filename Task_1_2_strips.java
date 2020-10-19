package s3757978.mapReduce.Assignment_2;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class Task_1_2_strips {
	
	//mapper class
	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, MapWritable> {

		// initiating logger 
		private static final Logger LOG = Logger.getLogger(TokenizerMapper.class);

		//declaring Text, DoubleWritable and MapWritable variables
		private MapWritable valueMap = new MapWritable();
		private Text word = new Text();
		private final static DoubleWritable one = new DoubleWritable(1);

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			LOG.setLevel(Level.DEBUG);
			LOG.debug("Mapper Task of APOORV BHARDWAJ, s3757978");

			String[] sentences = value.toString().split("\n");
			for (int i = 0 ; i < sentences.length ; i++) {

				int temp = sentences[i].length();

				if(temp > 0) {
					// splitting the text based on " " as delimiter
					String[] sentence = sentences[i].split(" ");
					for (int j = 0 ; j < sentence.length ; j++ ) {
						valueMap.clear();
						// replace all the non words
						String cleanTemp = sentence[j].replaceAll("\\W+","");
						//replace all the digits and convert to lower case
						cleanTemp = cleanTemp.replaceAll("\\d+","").toLowerCase();
						if(cleanTemp.length() > 1) {
							word.set(cleanTemp);

							for (int k = 0 ; k< sentence.length; k ++) {
								// skip the identical words
								if(j == k) {
									continue;
								}
								String cleanTemp2 = sentence[k].replaceAll("\\W+","");
								cleanTemp2 = cleanTemp2.replaceAll("\\d+","").toLowerCase();
								if(cleanTemp2.length() > 1) {
									// ensuring the size is >1
									Text contextWord = new Text(cleanTemp2);
									if (valueMap.containsKey(contextWord)) {
										DoubleWritable count = (DoubleWritable) valueMap.get(contextWord); 
										valueMap.put(contextWord,new DoubleWritable(count.get()+1));
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
		// declaring variables
		private MapWritable addedMap = new MapWritable();
		private double sum = 0;
		public void reduce(Text key, Iterable<MapWritable> values,
				Context context
				) throws IOException, InterruptedException {

			LOG.setLevel(Level.DEBUG);
			LOG.debug("Reducer Task of APOORV BHARDWAJ, s3757978");

			addedMap.clear();
			for(MapWritable value : values) {
				// addall to add all the maps belonging to a key
				// addAll is a custom method defined below
				addAll(value);
			}
			
			Set<Writable> keyset = addedMap.keySet();
			
			for (Writable key1 : keyset) {			
				DoubleWritable value = (DoubleWritable) addedMap.get(key1);
				sum += value.get();
			}
			
			// calculating the relative frequency by custom calcRf method
			calcRF(sum);
			context.write(key, addedMap);


		}

		private void calcRF( double sum) {
			
			Set<Writable> keys = addedMap.keySet();
			// looping over map
			for (Writable key : keys) {			
				DoubleWritable values = (DoubleWritable) addedMap.get(key);
				double rF = values.get()/sum;
				addedMap.put(key, new DoubleWritable(rF));			
			}
		}

		private void addAll(MapWritable mapWritable) {
			// method to loop and add all the values belonging to a particular key
			Set<Writable> keys = mapWritable.keySet();
			for (Writable key : keys) {
				DoubleWritable newCount = (DoubleWritable) mapWritable.get(key);
				if (addedMap.containsKey(key)) {
					DoubleWritable existingCount = (DoubleWritable) addedMap.get(key);				
					addedMap.put(key, new DoubleWritable(existingCount.get() + newCount.get()));
				} else {
					addedMap.put(key, newCount);
				}
			}
		}
	}
	
	//driver class
	public static void main(String[] args) throws Exception {

		// basic configuration
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Task_1_2_strips");
		job.setJarByClass(Task_1_2_strips.class);
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
