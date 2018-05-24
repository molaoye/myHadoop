package hadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

/**
 * 多表关联
 * 根据addressID关联求出factoryname-address表。很明显，左右关联即可，和单表关联一样。
 * 不多作表述，有需要可以查看单表关联的分析。
 *
 */
public class MTJoin {
	public static int times = 1;

	public static class MTMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String relation = new String();
			String line = value.toString();
			if (line.contains("factoryname") || line.contains("addressID"))
				return;
			int i = 0;
			while (line.charAt(i) < '0' || line.charAt(i) > '9') {
				i++;
			}
			if (i > 0) {// 左表
				relation = "1";
				context.write(new Text(String.valueOf(line.charAt(i))), new Text(relation + line.substring(0, i - 1)));
			} else {// 右表
				relation = "2";
				context.write(new Text(String.valueOf(line.charAt(i))), new Text(relation + line.substring(i + 1)));
			}

		}

	}

	public static class MTReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			if (times == 1) {
				context.write(new Text("factoryName"), new Text("Address"));
				times++;
			}
			int factoryNum = 0;
			int addressNum = 0;
			String[] factorys = new String[10];
			String[] addresses = new String[10];

			for (Text t : value) {
				if (t.charAt(0) == '1') {// 左表
					factorys[factoryNum] = t.toString().substring(1);
					factoryNum++;
				} else {// 右表
					addresses[addressNum] = t.toString().substring(1);
					addressNum++;
				}
			}

			for (int i = 0; i < factoryNum; i++) {
				for (int j = 0; j < addressNum; j++) {
					context.write(new Text(factorys[i]), new Text(addresses[j]));
				}
			}

		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 2) {
			System.out.println("Parameters error");
			System.exit(2);
		}

		Job job = new Job(conf, "MTjoin");
		job.setJarByClass(MTJoin.class);
		job.setMapperClass(MTMapper.class);
		job.setReducerClass(MTReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
