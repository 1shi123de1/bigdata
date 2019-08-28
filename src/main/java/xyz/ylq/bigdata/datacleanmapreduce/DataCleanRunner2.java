package xyz.ylq.bigdata.datacleanmapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataCleanRunner2 {

	public static void main(String[] args) throws Exception {
		// 参数长度判断
		if(args == null ||args.length<2){
			System.out.println("parameter length error!!");
			return;
		}
		Path outPath = new Path(args[args.length-1]);
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "DataCleanRunner2");
		job.setJarByClass(xyz.ylq.bigdata.datacleanmapreduce.DataCleanRunner2.class);
		job.setMapperClass(xyz.ylq.bigdata.datacleanmapreduce.DataCleanMapper2.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(0);
		
		for(int i = 0; i < args.length-1; i++){
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		// 删除output Path目录
		outPath.getFileSystem(conf).delete(outPath, true);
		FileOutputFormat.setOutputPath(job, outPath);
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
