package cs455.hadoop.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * This class implements Writable and contains term frequency, ngram count,
 * inverse document frequency, TFIDF, ngram, and decade span.
 * 
 * @author nljohnso
 * 
 */
public class NGramStatsWritable implements Writable {

	private DoubleWritable termFreq;
	private IntWritable nGramCount;
	private DoubleWritable invDocFreq;
	private DoubleWritable TFIDF;
	private Text nGram;
	private Text decade;

	public NGramStatsWritable() {
		this.termFreq = new DoubleWritable();
		this.nGramCount = new IntWritable();
		this.invDocFreq = new DoubleWritable();
		this.TFIDF = new DoubleWritable();
		this.nGram = new Text();
		this.decade = new Text();
	}

	public NGramStatsWritable(DoubleWritable termFreq, IntWritable nGramCount,
			DoubleWritable invDocFreq, DoubleWritable TFIDF, Text nGram,
			Text decade) {
		this.termFreq = termFreq;
		this.nGramCount = nGramCount;
		this.invDocFreq = invDocFreq;
		this.TFIDF = TFIDF;
		this.nGram = nGram;
		this.decade = decade;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		termFreq.readFields(dataInput);
		nGramCount.readFields(dataInput);
		invDocFreq.readFields(dataInput);
		TFIDF.readFields(dataInput);
		nGram.readFields(dataInput);
		decade.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		termFreq.write(dataOutput);
		nGramCount.write(dataOutput);
		invDocFreq.write(dataOutput);
		TFIDF.write(dataOutput);
		nGram.write(dataOutput);
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

	public DoubleWritable getInvDocFreq() {
		return invDocFreq;
	}

	public void setInvDocFreq(DoubleWritable invDocFreq) {
		this.invDocFreq = invDocFreq;
	}

	public DoubleWritable getTFIDF() {
		return TFIDF;
	}

	public void setTFIDF(DoubleWritable tFIDF) {
		TFIDF = tFIDF;
	}

	public Text getnGram() {
		return nGram;
	}

	public void setnGram(Text nGram) {
		this.nGram = nGram;
	}

	public Text getDecade() {
		return decade;
	}

	public void setDecade(Text decade) {
		this.decade = decade;
	}

	public String toString() {
		return "\n\tN-Gram: " + nGram
				+ "\n\tTerm-frequency: " + termFreq
				+ "\n\tInverse document frequency: " + invDocFreq
				+ "\n\tTF-IDF: " + TFIDF;
	}

}
