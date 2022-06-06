package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class HbaseDML {

    public static void main(String[] args) throws IOException {

        System.out.println(connection);

//        createTable("default","first","info1","info2");

//        createNamespace("test");

//        insertData(null,"student","1002","info1","addr","beijing");

        getData(null,"student","1001");

        deleteData(null,"student","1001","info1","name");

        scanData(null,"student","1001","1003");
    }

    //创建连接
    //设置静态属性hbase连接
    public static Connection connection = null;

    //创建连接的配置对象
    static {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //ddl-建namespace，需要admin连接
    public static void createNamespace(String namespace) throws IOException {
        Admin admin = connection.getAdmin();
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(java.lang.String.valueOf(namespace));
        NamespaceDescriptor build = builder.build();
        try {
        admin.createNamespace(build);
        }catch (NamespaceExistException a){
            System.out.println(namespace+"已存在，停止创建");
            return;
        }
        System.out.println(namespace+"创建成功");
        admin.close();
    }

    //ddl-建表，需要admin连接
    public static void createTable(String namespacename,String tablename,String...cfs) throws IOException {

        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(java.lang.String.valueOf(tablename));

        //判断表是否存在
        boolean result = admin.tableExists(tableName);
        if (result){
            System.err.println(namespacename==null?"default":namespacename+":"+tablename+"表已存在，停止创建");
            return;
        }

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);

        //判断列族个数
        if (cfs.length<1){
            System.err.println("至少要有一个列族");
        }

        //把列族信息添加进tableDescriptorBuilder
        for (String cf : cfs) {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes());
            ColumnFamilyDescriptor build = columnFamilyDescriptorBuilder.build();
            tableDescriptorBuilder.setColumnFamily(build);
        }

        TableDescriptor build = tableDescriptorBuilder.build();

        //建表
        admin.createTable(build);
        System.out.println(namespacename+":"+tablename+"表已成功创建");
        admin.close();
    }

    //dml-查询数据，需要table连接
    public static void getData(String spacename,String tablename,String rowkey) throws IOException {
        TableName tn = TableName.valueOf(spacename,tablename);
        Table table = connection.getTable(tn);

        Result result = table.get(new Get(Bytes.toBytes(rowkey)));
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            String cl = Bytes.toString(CellUtil.cloneRow(cell))+":"+
                    Bytes.toString(CellUtil.cloneFamily(cell))+":"+
                    Bytes.toString(CellUtil.cloneQualifier(cell))+":"+
                    Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println(cl);
        }

        table.close();
    }

    //dml-写入数据，需要table连接
    public static void insertData(String namespacename,String tablename,String rowkey,String columnfamily,String column,String value) throws IOException {

        TableName tn = TableName.valueOf(tablename);
        Table table = connection.getTable(tn.valueOf(namespacename,tablename));

        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(columnfamily),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);
    }

    //dml-扫描指定行数数据，需要table连接
    public static void scanData(String namespacename,String tablename,String startrow,String stoprow) throws IOException {

        TableName tn = TableName.valueOf(tablename);
        Table table = connection.getTable(tn.valueOf(namespacename, tablename));

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startrow)).withStopRow(Bytes.toBytes(stoprow));
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()){
            Result next = iterator.next();
            List<Cell> cells = next.listCells();
            for (Cell cell : cells) {
                String cl = Bytes.toString(CellUtil.cloneRow(cell))+":"+
                        Bytes.toString(CellUtil.cloneFamily(cell))+":"+
                        Bytes.toString(CellUtil.cloneQualifier(cell))+":"+
                        Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(cl);
            }
            System.out.println("---------------------------------");
        }
        table.close();
    }

    //dml-删除数据，需要table连接
    public static void deleteData(String namespacename,String tablename,String rowkey,String cf,String c) throws IOException {

        TableName tn = TableName.valueOf(tablename);
        Table table = connection.getTable(tn.valueOf(namespacename, tablename));

        Delete delete = new Delete(Bytes.toBytes(rowkey));
        delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(c));
        table.delete(delete);

        table.close();
    }

}
