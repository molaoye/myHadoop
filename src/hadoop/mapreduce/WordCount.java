package hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	// Ƕ���� Mapper
	// Mapper<keyin,valuein,keyout,valueout>
	public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		//һ��һ����
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);// Context����
			}
		}
	}

	// Ƕ����Reducer
	// Reduce<keyin,valuein,keyout,valueout>
	// Reducer��valuein����Ҫ��Mapper��valueout����һ��,Reducer��valuein��Mapper��valueout����shuffle֮���ֵ
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		//һ��һ����
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable i : values) {
				sum += i.get();
			}
			result.set(sum);
			context.write(key, result);// Context����
		}

	}

	public static void main(String[] args) throws Exception {
		// ���Configuration���� Configuration:core-default.xml,core-site.xml
		Configuration conf = new Configuration();

		// ����������args
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// �ж����������������Ϊ�����쳣�˳�
		if (otherArgs.length != 2) {
			System.err.println("Usage:wordcount <in> <out>");
			System.exit(2);
		}

		// ����Job����
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);// ��������оֲ��ϲ�
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// ����input path
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		
		// ����output path�����·��Ӧ��Ϊ�գ����򱨴�org.apache.hadoop.mapred.FileAlreadyExistsException��
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);// �Ƿ������˳�
	}

}
