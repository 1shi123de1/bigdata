package xyz.ylq.bigdata.datacleanmapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DataCleanRunner implements Tool{

//	static {
//	    try {
//	        // 设置 HADOOP_HOME 目录
//	        System.setProperty("hadoop.home.dir", "C:/manyTools/hadoop2.6_Win_x64");
//	        // 加载库文件
//	        System.load("C:/manyTools/hadoop2.6_Win_x64/hadoop.dll");
//	    } catch (UnsatisfiedLinkError e) {
//	        System.err.println("Native code library failed to load.\n" + e);
//	        System.exit(1);
//	    }
//	}
	
	private Configuration conf=null;
	public void setConf(Configuration conf) {
		this.conf=conf;
	}

	public Configuration getConf() {
		return this.conf;
	}

	public int run(String[] args) throws Exception {

		conf = this.getConf();
		conf.set("inpath",args[0]);
		conf.set("outpath",args[1]);
		
		Job job = Job.getInstance(conf, "dataclean");
		job.setJarByClass(DataCleanRunner.class);
		job.setMapperClass(DataCleanMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		
		this.initJobInputPath(job);
		this.initJobOutputPath(job);
		
		return job.waitForCompletion(true)?0:1;
	}

	private void initJobOutputPath(Job job) throws IOException, URISyntaxException {
		Configuration conf = job.getConfiguration();
		String outPathString = conf.get("outpath");
		
		FileSystem fSystem = FileSystem.get(new URI("hdfs://172.16.13.146:8020"), conf);
		
		Path outPath = new Path(outPathString);
		if(fSystem.exists(outPath)){
			fSystem.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);
	}

	private void initJobInputPath(Job job) throws IOException, URISyntaxException {
		Configuration conf = job.getConfiguration();
		String inPathString = conf.get("inpath");
		
		FileSystem fSystem = FileSystem.get(new URI("hdfs://172.16.13.146:8020"),conf);
		
		Path inPath = new Path(inPathString);
		if (fSystem.exists(inPath)) {
			FileInputFormat.addInputPath(job, inPath);
		}else{
			throw new RuntimeException("hdfs 中该文件目录不存在。"+inPath);
		}
	}
	
	public static void main(String[] args) {
		if(args == null ||args.length<2){
			System.out.println("parameter length error!!");
			return;
		}
		try{
			int resultCode = ToolRunner.run(new DataCleanRunner(), args);
			if(resultCode == 0){
				System.out.println("Success!");
			}else {
				System.out.println("File!");
			}
			System.exit(resultCode);
		}catch(Exception exception){
			exception.printStackTrace();
			System.exit(1);
		}
	}

}
