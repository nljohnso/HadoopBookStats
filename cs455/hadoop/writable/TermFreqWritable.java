package cs455.hadoop.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * This class implements Writable and contains term frequency, ngram count, most
 * frequent ngram, number of books in corpus, and decade span.
 * 
 * @author nljohnso
 * 
 */
public class TermFreqWritable implements Writable {

	private DoubleWritable termFreq;
	private IntWritable nGramCount;
	private IntWritable mostFreqNGram;
	private IntWritable numOfDocs;
	private Text decade;

	public TermFreqWritable() {
		this.termFreq = new DoubleWritable();
		this.nGramCount = new IntWritable();
		this.mostFreqNGram = new IntWritable();
		this.numOfDocs = new IntWritable();
		this.decade = new Text();
	}

	public TermFreqWritable(DoubleWritable termFreq, IntWritable nGramCount,
			IntWritable mostFreqNGram, IntWritable numOfDocs, Text decade) {
		this.termFreq = termFreq;
		this.nGramCount = nGramCount;
		this.mostFreqNGram = mostFreqNGram;
		this.numOfDocs = numOfDocs;
		this.decade = decade;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		termFreq.readFields(dataInput);
		nGramCount.readFields(dataInput);
		mostFreqNGram.readFields(dataInput);
		numOfDocs.readFields(dataInput);
		decade.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		termFreq.write(dataOutput);
		nGramCount.write(dataOutput);
		mostFreqNGram.write(dataOutput);
		numOfDocs.write(dataOutput);
		decade.write(dataOutput);
	}

	public DoubleWritable getTermFreq() {
		return termFreq;
	}

	public void setTermFreq(DoubleWritable termFreq) {
		this.termFreq = termFreq;
	}

	public IntWritable getnGramCount() {
		return nGramCount;
	}

	public void setnGramCount(IntWritable nGramCount) {
		this.nGramCount = nGramCount;
	}

	public IntWritable getMostFreqNGram() {
		return mostFreqNGram;
	}

	public void setMostFreqNGram(IntWritable mostFreqNGram) {
		this.mostFreqNGram = mostFreqNGram;
	}

	public IntWritable getNumOfDocs() {
		return numOfDocs;
	}

	public void setNumOfDocs(IntWritable numOfDocs) {
		this.numOfDocs = numOfDocs;
	}

	public Text getDecade() {
		return decade;
	}

	public void setDecade(Text decade) {
		this.decade = decade;
	}

	public String toString() {
		return " " + nGramCount + " " + termFreq + " " + numOfDocs + " "
				+ decade;
	}

}
