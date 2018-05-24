package hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 排序 利用MapReduce默认的对Key进行排序
 * 继承Partitioner类，重写getPartition使Mapper结果整体有序分到相应的Partition，输入到Reduce分别排序。
 * 利用全局变量统计位置
 *
 */
public class Sort {
	public static class SortMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("Key: " + key + "  " + "Value: " + value);
			//key为需要排序的值，value任意
			context.write(new IntWritable(Integer.valueOf(value.toString())), new IntWritable(1));

		}
	}

	public static class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		public static IntWritable lineNum = new IntWritable(1);// 记录该数据的位置

		// 查询value的个数，有多少个就输出多少个Key值。
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> value, Context context)
				throws IOException, InterruptedException {

			System.out.println("lineNum: " + lineNum);

			for (IntWritable i : value) {
				context.write(lineNum, key);
			}
			lineNum = new IntWritable(lineNum.get() + 1);
		}
	}

	public static class SortPartitioner extends Partitioner<IntWritable, IntWritable> {

		// 根据key对数据进行分派
		@Override
		public int getPartition(IntWritable key, IntWritable value, int partitionNum) {
			System.out.println("partitionNum: " + partitionNum);//此例执行不到这里
			// 输入的最大值，自己定义的。mapreduce自带的有采样算法和partition的实现可以用，此例没有用。
			int maxnum = 23492;
			int bound = maxnum / partitionNum;
			int keyNum = key.get();
			for (int i = 0; i < partitionNum; i++) {
				if (keyNum > bound * i && keyNum <= bound * (i + 1)) {
					return i;
				}
			}
			return -1;
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 2) {
			System.out.println("input parameters errors");
			System.exit(2);
		}

		Job job = new Job(conf);
		job.setJarByClass(Sort.class);
		job.setMapperClass(SortMapper.class);
		//注释掉也能排序？
//		job.setPartitionerClass(SortPartitioner.class);// 此例不许要combiner，需要设置Partitioner
		job.setReducerClass(SortReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
