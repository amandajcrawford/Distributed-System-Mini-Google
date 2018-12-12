import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class InvertedIndexMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private final static Text document = new Text();

	protected void map(LongWritable key, Text value, Context context)
			throws java.IOException, InterruptedException {

		String[] line = value.toString().split("=");
		String documentName = line[0];
		document.set(documentName);
		String textStr = line[1];
		String[] wordArray = textStr.split(" ");
		for(int i = 0; i <  wordArray.length; i++) { 

			wordText.set(wordArray[i]);

			context.write(word,document);

		}

	}

}