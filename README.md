USAGE
-----
```$HADOOP_HOME/bin/hadoop jar``` ```[Jar Name].jar``` ```cs455.hadoop.Job.BookAnalyzer``` ```[Input file/directory]``` ```[Reading Ease/Grade Level Output location]``` ```[NGram Frequencies Output location]```

BEFORE RUNNING
--------------
1.  Need to make sure intermediate files don't already exist. If they do change their names to something that don't exist in BookAnalyzer.java
2.  Change the number of files in your corpus in TermFreqMapper.java
3.  Change the ngram value in BookNGramMapper.java to 1,2,3, or 4.

cs455.hadoop.Job
----------------
**BookAnalyzer.java** - The main of the program which is responsible for creating jobs and their configurations.

cs455.hadoop.Mapper
-------------------
**BookStatsMapper.java** - This mapper parses each block of data given to it and writes to the reducer the following key value pair: <filename, BookStatsWritable>.

**BookNGramMapper.java** - This mapper also parses the file for a specified ngram value. It writes to the reducer the following key value pair: <filename + ngram, NGramWritable>.

**MostFreqNGramMapper.java** - This mapper parses the file outputted by the ngram parse job. It writes to the reducer the following key value pair: <filename, CommonNGramWritable>.

**TermFreqMapper.java** - This mapper parses the file outputted by the most frequent ngram calculation job.  It writes to the reducer the following key value pair: <filename + ngram, TermFreqWritable>.

**FreqDataMapper.java** - This mapper parses the file outputted by the ngram frequency calculation job.  It writes to the reducer the following key value pair: <ngram, FreqDataWritable>.

**TFIDFMapper.java** - This mapper parses the file outputted by the ngram document frequency calculation job.  It writes to the reducer the following key value pair: <filename + nGram, NGramStatsWritable>.

**NGramStatsMapper.java** - This is an optional mapper that parses the file outputted by the TFIDF calculation job.  It writes to the reducer the following key value pair: <decade + nGram, NGramStatsWritable>.

cs455.hadoop.Parser
-------------------
**Parser.java** - Uses regular expressions to parse a Sentence into words or ngrams.

**Sentence.java** - An object that contains a string that has ngram computation methods.

cs455.hadoop.Reducer
--------------------
**BookStatsReducer.java** - This reducer takes in filenames and sums up the total number of words, sentences, and syllables and then calculates the Flesch reading ease and grade level. It writes the following key value pair to the output file: <filename, BookStatsWritable>.

**BookNGramReducer.java** - This reducer takes in ngrams that have a specified book and sums up the total number of like-ngrams for that book. It writes the following key value pair to the output file: <filename + ngram, NGramWritable>.

**MostFreqNGramReducer.java** - This reducer takes in filenames and finds their most commonly occurring ngram.  It writes the following key value pair to the output file: <filename + ngram, CommonNGramWritable>.

**TermFreqReducer.java** - This reducer takes in ngrams that have a specified book and calculates it's term frequency for that book.  It writes the following key value pair to the output file: <filename + ngram, TermFreqWritable>.

**FreqDataReducer.java** - This reducer takes in a ngram and calculates how many books that ngram occurs in within the corpus.  It writes the following key value pair to the output file: <ngram + filename, FreqDataWritable>.

**TFIDFReducer.java** - This reducer takes in a ngram that has a specified book and calculates their inverse document frequencies and TF-IDF's. It writes the following key value pair to the output file: <filename, NGramStatsWritable>.

**NGramStatsMapper.java** - This reducer takes in a ngram that has a specified decade of publication and outputs the following key value pair to the output file: <decade, NGramStatsWritable>.

cs455.hadoop.Writable
---------------------

**BookStatsWritable.java** - This class implements Writable and contains word count, sentence count, syllable count, flesch reading ease, and flesch grade level.

**NGramWritable.java** - This class implements Writable and contains ngram count and decade span.

**CommonNGramWritable.java** - This class implements Writable and contains ngram, ngram count, most frequent ngram, and decade span.

**TermFreqWritable.java** - This class implements Writable and contains term frequency, ngram count, most frequent ngram, number of books in corpus, and decade span.

**FreqDataWritable.java** - This class implements Writable and contains filename, term frequency, ngram count, number of books, ngram book frequency, and decade span.

**TFIDFWritable.java** - This class implements Writable and contains term frequency, ngram count, number of books, ngram book frequency, inverse document frequency, TF-IDF, ngram, and decade span.

**NGramStatsWritable.java** - This class implements Writable and contains term frequency, ngram count, inverse document frequency, TFIDF, ngram, and decade span.
