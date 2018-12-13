import java.io.IOException;
import java.util.*;

import java.util.Iterator;
import java.util.StringTokenizer;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class WordCount {
	public static class InvertedIndex implements Writable {

		private HashMap<String, Integer> map = new HashMap<String, Integer>();

		public InvertedIndex() {
		}

		public InvertedIndex(HashMap<String, Integer> map) {

			this.map = map;
		}

		private Integer getCount(String tag) {
			return map.get(tag);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			Iterator<String> it = map.keySet().iterator();
			Text tag = new Text();

			while (it.hasNext()) {
				String t = it.next();
				tag = new Text(t);
				tag.readFields(in);
				new IntWritable(getCount(t)).readFields(in);
			}

		}

		@Override
		public void write(DataOutput out) throws IOException {
			Iterator<String> it = map.keySet().iterator();
			Text tag = new Text();
			IntWritable count = new IntWritable();

			while (it.hasNext()) {
				String t = it.next();
				new Text(t).write(out);
				new IntWritable(getCount(t)).write(out);
			}

		}

		@Override
		public String toString() {

			String output = "";

			for (String tag : map.keySet()) {
				output += (tag + ":" + getCount(tag).toString() + ",");
			}

			return output;

		}

	}

	public static class IndexMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			FileSplit filesplit = (FileSplit) reporter.getInputSplit();
			String fileName = filesplit.getPath().getName();

			String line = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();

				output.collect(new Text(token.toLowerCase()), new Text(fileName));
			}

		}
	}

	public static class IndexReducer extends MapReduceBase implements Reducer<Text, Text, Text, InvertedIndex> {

		private HashMap<String, Integer> map;

		private void add(String tag) {
			Integer val;

			if (map.get(tag) != null) {
				val = map.get(tag);
				map.remove(tag);
			} else {
				val = 0;
			}

			map.put(tag, val + 1);
		}

		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, InvertedIndex> output,
				Reporter reporter) throws IOException {
			map = new HashMap<String, Integer>();

			while (values.hasNext()) {
				add(values.next().toString());
			}

			output.collect(key, new InvertedIndex(map));

		}
	}

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(WordCount.class);

		conf.setJobName("WordCount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(conf, new Path("input"));
		FileOutputFormat.setOutputPath(conf, new Path("output"));

		conf.setMapperClass(IndexMapper.class);
		conf.setReducerClass(IndexReducer.class);

		client.setConf(conf);

		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}