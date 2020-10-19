package s3757978.mapReduce.Assignment_2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class Task_1_2_pairs {
	public static class TokenizerMapper
	extends Mapper<Object, Text, TextPair, DoubleWritable> {

		// initiating logger 
		private static final Logger LOG = Logger.getLogger(TokenizerMapper.class);

		//declaring TextPair and DoubleWritable variables
		private final static DoubleWritable one = new DoubleWritable(1);
		private TextPair textPair = new TextPair();

		//mapper class
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			LOG.setLevel(Level.DEBUG);
			LOG.debug("Mapper Task of APOORV BHARDWAJ, s3757978");


			String[] sentences = value.toString().split("\n");
			for (int i = 0 ; i < sentences.length ; i++) {

				int temp = sentences[i].length();

				if(temp > 1) {
					// splitting the text based on " " as delimiter
					String[] sentence = sentences[i].split(" ");

					for (int j = 0 ; j < sentence.length ; j++ ) {
						// removing non word
						String cleanTemp = sentence[j].replaceAll("\\W+","");
						// removing digits and converting to lower case
						cleanTemp = cleanTemp.replaceAll("\\d+","").toLowerCase();
						if(cleanTemp.length() > 1) {
							textPair.setFirst(new Text(cleanTemp));
							int frequency = 0;
							for (int k = 0 ; k< sentence.length; k ++) {
								String cleanTemp2 = sentence[k].replaceAll("\\W+","");
								cleanTemp2 = cleanTemp2.replaceAll("\\d+","").toLowerCase();
								if(j == k || cleanTemp.compareTo(cleanTemp2)==0 ) {
									continue;
								}								
								if(cleanTemp2.length() > 1) {
									textPair.setSecond(new Text(cleanTemp2));
									frequency ++;
									context.write(textPair, one);
								}

							}
							// writing to hdfs the wildcard variable useful for getting total count
							if(frequency > 0) {
								textPair.setSecond(new Text("***"));
								context.write(textPair, new DoubleWritable(frequency));
							}
							
						}


					}
				}

			}
		}
	}

	//reducer class
	public static class DoubleSumReducer
	extends Reducer<TextPair,DoubleWritable,TextPair,DoubleWritable> {
    	private DoubleWritable totalCount = new DoubleWritable();
		private DoubleWritable relativeCount = new DoubleWritable();
		private Text currentWord = new Text("NOT_SET");
		private Text flag = new Text("***");	// setting the wild card variable
		private static final Logger LOG = Logger.getLogger(DoubleSumReducer.class);
		public void reduce(TextPair key, Iterable<DoubleWritable> values,
				Context context
				) throws IOException, InterruptedException {

			LOG.setLevel(Level.DEBUG);
			LOG.debug("Reducer Task of APOORV BHARDWAJ, s3757978");
			
			// check if the wild card is present
			if (key.getSecond().equals(flag)) {
	                currentWord.set(key.getFirst());
	                totalCount.set(getTotalCount(values));
	            }
	        else {
	        	// calculate the relative count
	        	// get total count is a custom method defined below
	            double count = getTotalCount(values);
	            relativeCount.set((double) count / totalCount.get());
	            context.write(key, relativeCount);
	        }
		}
		
		 private double getTotalCount(Iterable<DoubleWritable> values) {
		        double count = 0;
		        for (DoubleWritable value : values) {
		            count += value.get();
		        }
		        return count;
		 }    
		
	}

	// partitioner class implemented for this task. 
	// numReduceTasks is set to 3
	// hashing is based on the first word in TextPair
	public static class Task_1_Partitioner extends
	Partitioner < TextPair, DoubleWritable >
	{
		@Override
		public int getPartition(TextPair key, DoubleWritable value, int numReduceTasks) {
			// TODO Auto-generated method stub
			return Math.abs(key.getFirst().hashCode() % numReduceTasks) ;

		}
	}
	
	// combiner class implemented for this task
	public static class PairsReducer extends Reducer<TextPair,DoubleWritable,TextPair,DoubleWritable> {
	    private DoubleWritable totalCount = new DoubleWritable();

	    @Override
	    protected void reduce(TextPair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
	        double count = 0;
	        for (DoubleWritable value : values) {
	             count += value.get();
	        }
	        totalCount.set(count);
	        context.write(key,totalCount);
	    }
	}
	
	// driver class
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Task_1_2_pairs");
		job.setJarByClass(Task_1_2_pairs.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(PairsReducer.class);
		job.setReducerClass(DoubleSumReducer.class);
		job.setPartitionerClass(Task_1_Partitioner.class);
		job.setNumReduceTasks(3);
		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(DoubleWritable.class);   // change it to DoubleWritable
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
