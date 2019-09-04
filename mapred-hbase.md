# bigdata
## 大数据组件demo
### CDH 环境下mapreduce 引入hbase jar包
mapreduce操作hbase时，需要导入hbase的相关jar包才能执行，否则会报ClassNotFoundExecption错误。
在CDH环境下，hadoop中加入hbase jar包操作如下：
* /etc/profile文件中，加入hadoop和hbase的home目录， *集群每个节点都要配置*
   ``` export HADOOP_HOME=/opt/cloudera/parcels/CDH-5.14.2-1.cdh5.14.2.p0.3/lib/hadoop
       export HBASE_HOME=/opt/cloudera/parcels/CDH-5.14.2-1.cdh5.14.2.p0.3/lib/hbase
   ```
   然后让配置生效 ``` source /etc/profile ```
* 编辑 vim /etc/hadoop/conf/hadoop-env.sh 文件，*集群每个节点都要配置*
   ```
       export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/opt/cloudera/parcels/CDH-5.14.2-1.cdh5.14.2.p0.3/lib/hbase/lib/*
   ```
   让配置生效  ``` source /etc/hadoop/conf/hadoop-env.sh ```
* 在CM中重启集群
 测试是否成功 </br>
  `yarn jar /opt/cloudera/parcels/CDH-5.14.2-1.cdh5.14.2.p0.3/lib/hbase/lib/hbase-server-1.2.0-cdh5.14.2.jar rowcounter fruit`
  没有出现ClassNotFoundExecption之类的找不到jar包错误就说明成功
