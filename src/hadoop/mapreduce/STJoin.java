package hadoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * �������

������

�����������������⡣���±�����child-parent���г�grandchild-grandparent���ֵ��

���������

��Ȼ��Ҫ�ֽ�Ϊ�������ű������������ӣ����������ű���ʵ����child-parent��ͨ��parent�ֶ���keyֵ�������ӡ�
���MapReduce�����ԣ�MapReduce����shuffle���̰���ͬ��key����һ�𴫵�Reduce���д���
OK��������˼·�ˣ�������parent��Ϊkey��������ұ��child��Ϊkey���������shuffle֮�����Ȼ�ģ�
���Ҿ�������һ���ˣ���ľ�У�Ȼ��ͨ�������ұ������Ͽ�������õ���������ݡ�
 *
 */
public class STJoin {
	public static int time = 0;

	public static class STJoinMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String childName = new String();
			String parentName = new String();
			String relation = new String();
			String line = value.toString();
			int i = 0;
			while (line.charAt(i) != ' ') {
				i++;
			}
			String[] values = { line.substring(0, i), line.substring(i + 1) };
			if (values[0].compareTo("child") != 0) {
				childName = values[0];
				parentName = values[1];
				relation = "1";// ���ұ������־
				context.write(new Text(parentName), new Text(relation + "+" + childName));// ���
				relation = "2";
				context.write(new Text(childName), new Text(relation + "+" + parentName));// �ұ�
			}
		}
	}

	public static class STJoinReduce extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			if (time == 0) {// �����ͷ
				context.write(new Text("grandChild"), new Text("grandParent"));
				time++;
			}
			int grandChildNum = 0;
			String[] grandChild = new String[10];
			int grandParentNum = 0;
			String[] grandParent = new String[10];
			Iterator<Text> ite = values.iterator();
			while (ite.hasNext()) {
				String record = ite.next().toString();
				int len = record.length();
				int i = 2;
				if (len == 0)
					continue;
				char relation = record.charAt(0);

				if (relation == '1') {// �������child
					String childName = new String();
					while (i < len) {// ����name
						childName = childName + record.charAt(i);
						i++;
					}
					grandChild[grandChildNum] = childName;
					grandChildNum++;
				} else {// ���ұ���parent
					String parentName = new String();
					while (i < len) {// ����name
						parentName = parentName + record.charAt(i);
						i++;
					}
					grandParent[grandParentNum] = parentName;
					grandParentNum++;
				}
			}
			// ����������Ͽ�����
			if (grandChildNum != 0 && grandParentNum != 0) {
				for (int m = 0; m < grandChildNum; m++) {
					for (int n = 0; n < grandParentNum; n++) {
						System.out.println("grandChild " + grandChild[m] + " grandParent " + grandParent[n]);
						context.write(new Text(grandChild[m]), new Text(grandParent[n]));
					}
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.out.println("parameter error");
			System.exit(2);
		}

		Job job = new Job(conf);
		job.setJarByClass(STJoin.class);
		job.setMapperClass(STJoinMapper.class);
		job.setReducerClass(STJoinReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
