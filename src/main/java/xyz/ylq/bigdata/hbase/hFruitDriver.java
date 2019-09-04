package xyz.ylq.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * driver
 * 把hdfs文件系统上的文件加载到hbase表中
 * 第一个参数是文件所在路径
 * 第二个参数是表名
 */
public class hFruitDriver implements Tool{
	
	private Configuration configuration = null;

	public void setConf(Configuration conf) {
		configuration=  conf;
	}

	public Configuration getConf() {
		return configuration;
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(configuration);
		
		// set driver class
		job.setJarByClass(hFruitDriver.class);
		
		// set mapper & input output 
		job.setMapperClass(hFruitMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		// set reducer
		TableMapReduceUtil.initTableReducerJob(args[1], hFruitReducer.class, job);
		
		// set input arg
		FileInputFormat.setInputPaths(job, args[0]);
		
		// submit job
		boolean result = job.waitForCompletion(true);
		
		return result ? 0:1;
	}

	public static void main(String[] args) {
		
		try {
			Configuration configuration = new Configuration();
			int run = ToolRunner.run(configuration, new hFruitDriver(), args);
			System.exit(run);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
