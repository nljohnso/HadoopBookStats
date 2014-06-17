package cs455.hadoop.Job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cs455.hadoop.Mapper.BookNGramMapper;
import cs455.hadoop.Mapper.BookStatsMapper;
import cs455.hadoop.Mapper.FreqDataMapper;
import cs455.hadoop.Mapper.MostFreqNGramMapper;
import cs455.hadoop.Mapper.NGramStatsMapper;
import cs455.hadoop.Mapper.TFIDFMapper;
import cs455.hadoop.Mapper.TermFreqMapper;
import cs455.hadoop.Reducer.BookNGramReducer;
import cs455.hadoop.Reducer.BookStatsReducer;
import cs455.hadoop.Reducer.FreqDataReducer;
import cs455.hadoop.Reducer.MostFreqNGramReducer;
import cs455.hadoop.Reducer.NGramStatsReducer;
import cs455.hadoop.Reducer.TFIDFReducer;
import cs455.hadoop.Reducer.TermFreqReducer;
import cs455.hadoop.Writable.BookStatsWritable;
import cs455.hadoop.Writable.CommonNGramWritable;
import cs455.hadoop.Writable.FreqDataWritable;
import cs455.hadoop.Writable.NGramStatsWritable;
import cs455.hadoop.Writable.TFIDFWritable;
import cs455.hadoop.Writable.NGramWritable;
import cs455.hadoop.Writable.TermFreqWritable;

public class BookAnalyzer {
	
	private static final boolean YEAR_SPAN = false;
	private static final String NGRAM_OUTPUT = "ngram_output3";
	private static final String MOST_COMMON_NGRAM_OUTPUT = "most_common_ngram3";
	private static final String TERM_FREQ_OUTPUT = "term_freq3";
	private static final String FREQ_DATA_OUTPUT = "freq_data3";
	private static final String TFIDF_OUTPUT = "tf_idf3";

	/**
	 * The main of the program which is responsible for creating jobs and their configurations.
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		//READING EASE AND GRADE LEVEL JOB
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(BookAnalyzer.class);
		job.setJobName("Knowledge Extraction");

		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status_list = fs.listStatus(new Path(args[0]));
		if (status_list != null) {
			for (FileStatus status : status_list) {
				FileInputFormat.addInputPath(job, status.getPath());
			}
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(BookStatsMapper.class);
		job.setReducerClass(BookStatsReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BookStatsWritable.class);
		
		job.waitForCompletion(true);
		
		//NGRAM JOB---------------------------------
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2);
		job2.setJarByClass(BookAnalyzer.class);
		job2.setJobName("N-Gram Extraction");
		Path nGramOutput = new Path(NGRAM_OUTPUT);
		

		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, nGramOutput);

		job2.setMapperClass(BookNGramMapper.class);
		job2.setReducerClass(BookNGramReducer.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NGramWritable.class);
		
		job2.waitForCompletion(true);
			
		//MOST COMMON NGRAM PER DOCUMENT
		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3);
		job3.setJarByClass(BookAnalyzer.class);
		job3.setJobName("Most-Common-NGram");
		Path commonNGramOutput = new Path(MOST_COMMON_NGRAM_OUTPUT);
		

		FileInputFormat.addInputPath(job3, nGramOutput);
		FileOutputFormat.setOutputPath(job3, commonNGramOutput);

		job3.setMapperClass(MostFreqNGramMapper.class);
		job3.setReducerClass(MostFreqNGramReducer.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(CommonNGramWritable.class);
		
		job3.waitForCompletion(true);
		
		//TERM FREQUENCY
		Configuration conf4 = new Configuration();
		Job job4 = Job.getInstance(conf4);
		job4.setJarByClass(BookAnalyzer.class);
		job4.setJobName("Term-frequency");
		Path termFreqOutput = new Path(TERM_FREQ_OUTPUT);
		

		FileInputFormat.addInputPath(job4, commonNGramOutput);
		FileOutputFormat.setOutputPath(job4, termFreqOutput);

		job4.setMapperClass(TermFreqMapper.class);
		job4.setReducerClass(TermFreqReducer.class);

		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(TermFreqWritable.class);
		
		job4.waitForCompletion(true);
		
		//GET INVERSE FREQUENCY DATA
		Configuration conf5 = new Configuration();
		Job job5 = Job.getInstance(conf5);
		job5.setJarByClass(BookAnalyzer.class);
		job5.setJobName("Freq-data");
		Path freqDataOutput = new Path(FREQ_DATA_OUTPUT);
		

		FileInputFormat.addInputPath(job5, termFreqOutput);
		FileOutputFormat.setOutputPath(job5, freqDataOutput);

		job5.setMapperClass(FreqDataMapper.class);
		job5.setReducerClass(FreqDataReducer.class);

		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(FreqDataWritable.class);
		
		job5.waitForCompletion(true);
		
		if(YEAR_SPAN) {
			//GET TERM FREQUENCY STATS
			Configuration conf6 = new Configuration();
			Job job6 = Job.getInstance(conf6);
			job6.setJarByClass(BookAnalyzer.class);
			job6.setJobName("Term-freq-stats");
			Path TFIDFOutput = new Path(TFIDF_OUTPUT);
	
			FileInputFormat.addInputPath(job6, freqDataOutput);
			FileOutputFormat.setOutputPath(job6, TFIDFOutput);
	
			job6.setMapperClass(TFIDFMapper.class);
			job6.setReducerClass(TFIDFReducer.class);
	
			job6.setOutputKeyClass(Text.class);
			job6.setOutputValueClass(TFIDFWritable.class);
			
			job6.waitForCompletion(true);
			
			//GET DECADE STATS
			Configuration conf7 = new Configuration();
			Job job7 = Job.getInstance(conf7);
			job7.setJarByClass(BookAnalyzer.class);
			job7.setJobName("Decade-stats");
			
	
			FileInputFormat.addInputPath(job7, TFIDFOutput);
			FileOutputFormat.setOutputPath(job7, new Path(args[2]));
	
			job7.setMapperClass(NGramStatsMapper.class);
			job7.setReducerClass(NGramStatsReducer.class);
	
			job7.setOutputKeyClass(Text.class);
			job7.setOutputValueClass(NGramStatsWritable.class);
			
			System.exit(job7.waitForCompletion(true) ? 0 : 1);
			
		} else {
			
			//GET TERM FREQUENCY STATS
			Configuration conf6 = new Configuration();
			Job job6 = Job.getInstance(conf6);
			job6.setJarByClass(BookAnalyzer.class);
			job6.setJobName("Term-freq-stats");

			FileInputFormat.addInputPath(job6, freqDataOutput);
			FileOutputFormat.setOutputPath(job6, new Path(args[2]));

			job6.setMapperClass(TFIDFMapper.class);
			job6.setReducerClass(TFIDFReducer.class);

			job6.setOutputKeyClass(Text.class);
			job6.setOutputValueClass(TFIDFWritable.class);
			
			job6.waitForCompletion(true);
			
			System.exit(job6.waitForCompletion(true) ? 0 : 1);
		}
				
	}
}
