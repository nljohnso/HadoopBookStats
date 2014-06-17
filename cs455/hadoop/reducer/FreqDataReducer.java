package cs455.hadoop.Reducer;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.hadoop.Writable.FreqDataWritable;

public class FreqDataReducer extends Reducer<Text, FreqDataWritable, Text, FreqDataWritable>{
	public void reduce(Text key, Iterable<FreqDataWritable> values,
			Context context) throws IOException, InterruptedException {
		
		int nGramDocFreq = 0;
		
		
		Text filename = null;
		DoubleWritable nGramFreq = null;
		IntWritable nGramCount = null;
		IntWritable numOfDocs = null;
		Text decade = null;
		
		FreqDataWritable output = new FreqDataWritable();
		
		ArrayList<Text> filenames = new ArrayList<Text>();
		ArrayList<IntWritable> nGramCounts = new ArrayList<IntWritable>();
		ArrayList<DoubleWritable> nGramFreqs = new ArrayList<DoubleWritable>();
		ArrayList<IntWritable> totalDocs = new ArrayList<IntWritable>();
		ArrayList<Text> decades = new ArrayList<Text>();
		

		for(FreqDataWritable i : values) {
			filenames.add(new Text(i.getFilename()));
			nGramCounts.add(new IntWritable(i.getnGramCount().get()));
			nGramFreqs.add(new DoubleWritable(i.getTermFreq().get()));
			totalDocs.add(new IntWritable(i.getNumOfDocs().get()));
			decades.add(new Text(i.getDecade()));
		}
	
		
		for(int i = 0; i < filenames.size(); i++) {
			
			filename = filenames.get(i);
			nGramFreq = nGramFreqs.get(i);
			nGramCount = nGramCounts.get(i);
			numOfDocs = totalDocs.get(i);
			nGramDocFreq = filenames.size();
			decade = decades.get(i);
			
			output.setFilename(filename);
			output.setTermFreq(nGramFreq);
			output.setnGramCount(nGramCount);
			output.setNumOfDocs(numOfDocs);
			output.setnGramDocFreq(new IntWritable(nGramDocFreq));
			output.setDecade(decade);
			
			context.write(new Text(key + " " + filenames.get(i)), output);
			
		}
	}
}
