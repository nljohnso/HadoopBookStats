package cs455.hadoop.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * This class implements Writable and contains filename, term frequency, ngram
 * count, number of books, ngram book frequency, and decade span.
 * 
 * @author nljohnso
 * 
 */
public class FreqDataWritable implements Writable {

	private Text filename;
	private DoubleWritable termFreq;
	private IntWritable nGramCount;
	private IntWritable numOfDocs;
	private IntWritable nGramDocFreq;
	private Text decade;

	public FreqDataWritable() {
		this.filename = new Text();
		this.termFreq = new DoubleWritable();
		this.nGramCount = new IntWritable();
		this.numOfDocs = new IntWritable();
		this.nGramDocFreq = new IntWritable();
		this.decade = new Text();
	}

	public FreqDataWritable(FreqDataWritable FDW) {
		this.filename = FDW.getFilename();
		this.termFreq = FDW.getTermFreq();
		this.nGramCount = FDW.getnGramCount();
		this.numOfDocs = FDW.getNumOfDocs();
		this.nGramDocFreq = FDW.getnGramDocFreq();
		this.decade = FDW.getDecade();
	}

	public FreqDataWritable(Text filename, DoubleWritable termFreq,
			IntWritable nGramCount, IntWritable numOfDocs,
			IntWritable nGramDocFreq, Text decade) {
		this.filename = filename;
		this.termFreq = termFreq;
		this.nGramCount = nGramCount;
		this.numOfDocs = numOfDocs;
		this.nGramDocFreq = nGramDocFreq;
		this.decade = decade;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		filename.readFields(dataInput);
		termFreq.readFields(dataInput);
		nGramCount.readFields(dataInput);
		numOfDocs.readFields(dataInput);
		nGramDocFreq.readFields(dataInput);
		decade.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		filename.write(dataOutput);
		termFreq.write(dataOutput);
		nGramCount.write(dataOutput);
		numOfDocs.write(dataOutput);
		nGramDocFreq.write(dataOutput);
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

	public IntWritable getNumOfDocs() {
		return numOfDocs;
	}

	public void setNumOfDocs(IntWritable numOfDocs) {
		this.numOfDocs = numOfDocs;
	}

	public Text getFilename() {
		return filename;
	}

	public void setFilename(Text filename) {
		this.filename = filename;
	}

	public IntWritable getnGramDocFreq() {
		return nGramDocFreq;
	}

	public void setnGramDocFreq(IntWritable nGramDocFreq) {
		this.nGramDocFreq = nGramDocFreq;
	}

	public Text getDecade() {
		return decade;
	}

	public void setDecade(Text decade) {
		this.decade = decade;
	}

	public String toString() {
		return " " + nGramCount + " " + termFreq + " " + numOfDocs + " "
				+ nGramDocFreq + " " + decade;
	}

}
