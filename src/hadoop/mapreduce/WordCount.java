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

	// 嵌套类 Mapper
	// Mapper<keyin,valuein,keyout,valueout>
	public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		//一行一行来
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);// Context机制
			}
		}
	}

	// 嵌套类Reducer
	// Reduce<keyin,valuein,keyout,valueout>
	// Reducer的valuein类型要和Mapper的valueout类型一致,Reducer的valuein是Mapper的valueout经过shuffle之后的值
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		//一行一行来
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable i : values) {
				sum += i.get();
			}
			result.set(sum);
			context.write(key, result);// Context机制
		}

	}

	public static void main(String[] args) throws Exception {
		// 获得Configuration配置 Configuration:core-default.xml,core-site.xml
		Configuration conf = new Configuration();

		// 获得输入参数args
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// 判断输入参数个数，不为两个异常退出
		if (otherArgs.length != 2) {
			System.err.println("Usage:wordcount <in> <out>");
			System.exit(2);
		}

		// 设置Job属性
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);// 将结果进行局部合并
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 传入input path
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		
		// 传入output path，输出路径应该为空，否则报错org.apache.hadoop.mapred.FileAlreadyExistsException。
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);// 是否正常退出
	}

}
