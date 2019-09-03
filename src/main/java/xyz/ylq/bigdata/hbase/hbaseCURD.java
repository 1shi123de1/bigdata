package xyz.ylq.bigdata.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class hbaseCURD {

	private static Connection connection = null;
	private static Admin admin = null;
	static{
		try {
			//配置信息
			Configuration configuration = HBaseConfiguration.create();
			configuration.set("hbase.zookeeper.quorum", "gwnet01,gwnet02,gwnet03");
			//创建连接
			connection = ConnectionFactory.createConnection(configuration);
			// 创建admin对象
			admin = connection.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// 0.关闭资源
	public static void close() {
		if(admin!=null){
			try {
				admin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if(connection!=null){
			try {
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	// 1.判断表是否存在
	public static boolean isTableExist(String tableName) throws IOException {
		boolean tableExist = admin.tableExists(TableName.valueOf(tableName));
		return tableExist;
	}
	
	// 2.创建表
	public static void createTable(String tableName,String... columnFamily) throws IOException{
		//判断表是否存在
		if(isTableExist(tableName)){
			System.out.println(tableName+" 已存在！");
			return;
		}
		if(columnFamily.length<=0){
			System.out.println("参数不够，请添加列族名！");
			return;
		}
		
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		for( String cf : columnFamily){
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
			tableDescriptor.addFamily(hColumnDescriptor);
		}
		
		admin.createTable(tableDescriptor);
	}
	
	// 3.删除表
	public static void dropTable(String tableName) throws IOException{
		//判断表是否存在
		if(!isTableExist(tableName)){
			System.out.println(tableName+" 不存在！");
			return;
		}
		// 先disable 表,在删除表
		admin.disableTable(TableName.valueOf(tableName));
		admin.deleteTable(TableName.valueOf(tableName));
	}
	
	// 4.创建命名空间
	public static void createNameSpace(String ns){
		NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
		
		try {
			admin.createNamespace(namespaceDescriptor);
		}catch(NamespaceExistException namespaceExistException){
			System.out.println(ns+" 已存在 ");
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// 5.插入数据
	public static void putData(String tableName,String rowKey,String cf,String qualifier,String value) throws IOException{
		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		
		table.put(put);
		table.close();
	}
	
	// 6.获取数据
	public static void getData(String tableName,String rowKey,String cf,String qualifier) throws IOException {
		if(!isTableExist(tableName)){
			System.out.println(tableName+" no exists!");
			return;
		}
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		Get get = new Get(Bytes.toBytes(rowKey));
		//指定列族
//		get.addFamily(Bytes.toBytes(cf));
		//指定列
//		get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier));
		//指定最大版本
//		get.setMaxVersions();
		Result result = table.get(get);
		
		for(Cell cell : result.rawCells()){
			System.out.println("ColumnFamily:"+Bytes.toString(CellUtil.cloneFamily(cell))+
							   ",Qualifier:"+Bytes.toString(CellUtil.cloneQualifier(cell))+
							   ",Value:"+Bytes.toString(CellUtil.cloneValue(cell)));
		}
		
		table.close();
	}
	
	// 7. scan方式获取数据 
	public static void scanTable(String tableName) throws IOException{
		if(!isTableExist(tableName)){
			System.out.println(tableName+" no exists!");
			return;
		}
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		Scan scan = new Scan();
		ResultScanner resultScanner = table.getScanner(scan);
		
		for(Result result : resultScanner){
			for(Cell cell : result.rawCells()){
				System.out.println("rowKey:"+Bytes.toString(CellUtil.cloneRow(cell))+
								   ",ColumnFamily:"+Bytes.toString(CellUtil.cloneFamily(cell))+
								   ",Qualifier:"+Bytes.toString(CellUtil.cloneQualifier(cell))+
								   ",Value:"+Bytes.toString(CellUtil.cloneValue(cell))
								   );
			}
		}
	}
	
	// 8.删除数据
	public static void deleteData(String tableName,String rowKey,String cf,String qualifier) throws IOException{
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		
		// 慎用delete.addColumn(cf,qualifier)方法，可能会出现 删除一个数据后出现上一个版本的数据 问题
		// 删除可以只指定列族或者只指定rowKey
		delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(qualifier));
		
		table.delete(delete);
		table.close();
	}
	
	public static void main(String[] args) throws IOException {
//		System.out.println(isTableExist("test"));
//		
//		createTable("test", "info1","info2");
//		
//		dropTable("test");
//		System.out.println(isTableExist("test"));
//		createNameSpace("ylq");
//		createTable("ylq:testTable", "info");
		scanTable("ylq:testTable2");
		close();
	}

}
