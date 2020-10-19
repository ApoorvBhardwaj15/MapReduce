package s3757978.mapReduce.Assignment_2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;



public class Task_1_1_pairs {

	//mapper class
	public static class TokenizerMapper
	extends Mapper<Object, Text, TextPair, IntWritable> {

		// initiating logger 
		private static final Logger LOG = Logger.getLogger(TokenizerMapper.class);

		//declaring TextPair and IntWritable variables
		// TextPair is custom writable class
		private final static IntWritable one = new IntWritable(1);
		private TextPair textPair = new TextPair();

		
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			LOG.setLevel(Level.DEBUG);
			LOG.debug("Mapper Task of APOORV BHARDWAJ, s3757978");

			

			String[] sentences = value.toString().split("\n");
			for (int i = 0 ; i < sentences.length ; i++) {

				int temp = sentences[i].length();

				if(temp > 1) {
					String[] sentence = sentences[i].split(" "); 

					for (int j = 0 ; j < sentence.length ; j++ ) {
						String cleanTemp = sentence[j].replaceAll("\\W+","");  //removing non words
						cleanTemp = cleanTemp.replaceAll("\\d+","").toLowerCase(); // removing digits and converting to lower case
						if(cleanTemp.length() > 1) {				// ensuring TextPair is not null
							textPair.setFirst(new Text(cleanTemp));
							for (int k = 0 ; k< sentence.length; k ++) {
								String cleanTemp2 = sentence[k].replaceAll("\\W+","");
								cleanTemp2 = cleanTemp2.replaceAll("\\d+","").toLowerCase();
								if(j == k || cleanTemp.compareTo(cleanTemp2)==0 ) {  // avoiding similar words in TextPair
									continue;
								}
								if(cleanTemp2.length() > 1) {						//ensuring size is >1
									textPair.setSecond(new Text(cleanTemp2));
									context.write(textPair, one);			//writing to hdfs
								}

							}
						}
					}
				}
			}
		}
	}

	// reducer class
	public static class IntSumReducer
	extends Reducer<TextPair,IntWritable,TextPair,IntWritable> {

		// initate logger
		private static final Logger LOG = Logger.getLogger(IntSumReducer.class);

		private IntWritable result = new IntWritable();

		public void reduce(TextPair key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {

			LOG.setLevel(Level.DEBUG);
			LOG.debug("Reducer Task of APOORV BHARDWAJ, s3757978");

			int sum = 0;
			for (IntWritable val : values) {	// looping over and summing the values
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);			//writing to hdfs
		}
	}

	//Partitioner class
	public static class Task_1_Partitioner extends
	Partitioner < TextPair, IntWritable >
	{
		@Override
		public int getPartition(TextPair key, IntWritable value, int numReduceTasks) {
			
			return Math.abs(key.getFirst().hashCode() % numReduceTasks) ;  //hashing based on first word

		}
	}

	// driver class
	public static void main(String[] args) throws Exception {

		// basic configuration
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Task_1_1_pairs");
		job.setJarByClass(Task_1_1_pairs.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setPartitionerClass(Task_1_Partitioner.class);
		job.setNumReduceTasks(3);
		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
