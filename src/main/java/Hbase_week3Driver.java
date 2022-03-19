import dto.StudentBasicInfo;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author YangChunping
 * @version 1.0
 * @date 2022/3/15 8:55
 * @description
 */
public class Hbase_week3Driver {

    public static void main(String[] args) throws IOException {
        //创建表
        String tableName = "ChloeStudent";
        Connection connection = init();
        System.out.println("connection : "+ connection);
        createTable(connection, tableName, buildColumnFamily());
        Map<String,  Map<String, Map<String, String>>> data = initData();

        saveData(connection,tableName, data);
        System.out.println("save successfully");
        List<Map<String, String>> returnedResult = getData(connection,tableName, "1", "5", buildColumnFamilyAndColumn());
        for(Map<String, String> result : returnedResult){
            System.out.println("result :" + result);
            System.out.println("rowKey :" + result.get("rowKey"));
            System.out.println("name : " + result.get("name"));
            System.out.println("programming : " + result.get("programming"));
            System.out.println("understanding : " + result.get("class"));
            System.out.println("studentId : " + result.get("student_id"));


        }
        System.out.println("get successfully");
        List<String> rowIds = new ArrayList<>();
        rowIds.add("1");
        deleteData(connection, tableName, rowIds);
        dropTable(connection, tableName);
        System.out.println("drop successfully");
    }

    public static Map<String, List<String>> buildColumnFamilyAndColumn(){
        Map<String, List<String>> columnFaimilyAndColumns = new HashMap<>();
        return columnFaimilyAndColumns;
    }



    private static Map<String,  Map<String, Map<String, String>>> initData() {
        StudentBasicInfo student1 = StudentBasicInfo.builder().studentId("20210000000001").
                studentClass("1").understanding("75").programming("82").name("Tom").build();

        StudentBasicInfo student2 = StudentBasicInfo.builder().studentId("20210000000002").
                studentClass("1").understanding("85").programming("67").name("Jerry").build();

        StudentBasicInfo student3 = StudentBasicInfo.builder().studentId("20210000000003").
                studentClass("2").understanding("80").programming("80").name("Jack").build();

        StudentBasicInfo student4 = StudentBasicInfo.builder().studentId("20210000000004").
                studentClass("2").understanding("60").programming("61").name("Rose").build();

        StudentBasicInfo student5 = StudentBasicInfo.builder().studentId("G20210698040228").
                studentClass("2").understanding("60").programming("61").name("杨春萍").build();

        List<StudentBasicInfo> basicInfos = new ArrayList<>();
        basicInfos.add(student1);
        basicInfos.add(student2);
        basicInfos.add(student3);
        basicInfos.add(student4);
        basicInfos.add(student5);
       return buildColums(basicInfos);
    }

    private static  Map<String,  Map<String, Map<String, String>>> buildColums(List<StudentBasicInfo> list) {
        Map<String,  Map<String, Map<String, String>>> line = new HashMap<>();
        Map<String, Map<String, String>> columnFamilies = new HashMap<>();
        Map<String, String> columnsName = new HashMap<>();
        Map<String, String> columnsInfo = new HashMap<>();
        Map<String, String> columnsScore = new HashMap<>();
        int i = 1;
        for(StudentBasicInfo studentBasicInfo : list){
            columnsName.put("name", studentBasicInfo.getName());
            columnsInfo.put("student_id", studentBasicInfo.getStudentId());
            columnsInfo.put("class", studentBasicInfo.getStudentClass());
            columnsScore.put("understanding", studentBasicInfo.getUnderstanding());
            columnsScore.put("programming", studentBasicInfo.getProgramming());

            columnFamilies.put("name", columnsName);
            columnFamilies.put("info", columnsInfo);
            columnFamilies.put("score", columnsScore);

            String rowId = String.valueOf(i);
            i++;

            line.put(rowId, columnFamilies);

        }


        return line;

    }

    private static List<String> buildColumnFamily(){
        List<String> columnFamily = new ArrayList<>();
        columnFamily.add("name");
        columnFamily.add("info");
        columnFamily.add("score");
        return columnFamily;
    }

    private static Connection init() throws IOException {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.set("hbase.zookeeper.quorum", "localhost");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "127.0.0.1:60000");
        //configuration.set("hbase.cluster.distributed", "false");
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    public static boolean createTable(Connection connection, String tableName, List<String> columnFaimily){
        try(Admin adminHbase =connection.getAdmin()) {
            System.out.println("admin : " + adminHbase);
            TableName name = TableName.valueOf(tableName);
//            boolean exists = adminHbase.tableExists(name);
//            System.out.println("exist"+exists);
//            if(exists){
//                return true;
//            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(name);
            System.out.println("tableDescriptorBuilder : " + tableDescriptorBuilder);
            System.out.println("columnFaimily : " + columnFaimily);
            for(String cf : columnFaimily){
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).setCompressTags(true)
                        .setMaxVersions(1).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }
            tableDescriptorBuilder.setDurability(Durability.SYNC_WAL);
            tableDescriptorBuilder.setCompactionEnabled(true);
            System.out.println("tableDescriptorBuilder : " + tableDescriptorBuilder);
            adminHbase.createTable(tableDescriptorBuilder.build());
            return true;
        }
        catch (Exception e){
            System.out.println("exception : " + e);
        }
        return false;
    }

    public static boolean dropTable(Connection connection, String tableName){
        try(Admin adminHbase =connection.getAdmin()) {
            TableName table = TableName.valueOf(tableName);
            if(adminHbase.tableExists(table)){
                adminHbase.disableTable(table);
                adminHbase.deleteTable(table);
            }

        } catch (IOException exception){
            System.out.println("exception : " + exception);
        }
        return false;
    }

    public static List<Map<String, String>> getData(Connection connection, String tableName, String startRowKey, String endRowKey, Map<String, List<String>> columnFamilies){
        List<Map<String, String>> list = new ArrayList<>();
        try(Admin adminHbase = connection.getAdmin()){
            TableName table = TableName.valueOf(tableName);
            if(adminHbase.tableExists(table)){
                Table hbaseTable = connection.getTable(table);
                List<Result> result = scanRows(hbaseTable, startRowKey, endRowKey,columnFamilies);
                result.forEach(row->{
                    Map<String, String> map = new HashMap<>();
                    map.put("rowKey", Bytes.toString(row.getRow()));
                    for(Cell cell :row.listCells()){
                        map.put(Bytes.toString(CellUtil.cloneQualifier(cell)),
                                Bytes.toString(CellUtil.cloneValue(cell)));
                    }
                    list.add(map);
                });

            }
        } catch (IOException exception){
            System.out.println(exception);
        }
        return list;

    }

    private static List<Result> scanRows(Table table, String startRowKey, String endRowKey, Map<String, List<String>> columnFamilies) throws IOException {
        List<Result> result = new ArrayList<>();
        Scan scanVo = buildScan(startRowKey, endRowKey, columnFamilies);
        ResultScanner scanResult = table.getScanner(scanVo);
        for(Result row : scanResult){
            result.add(row);
        }
        return result;
    }

    private static Scan buildScan(String startRowKey, String endRowKey, Map<String, List<String>> columnFamilies) {
        Scan scanVo = new Scan();
        scanVo.withStartRow(Bytes.toBytes(startRowKey), true);
        scanVo.withStopRow(Bytes.toBytes(endRowKey), false);
        if(columnFamilies != null && columnFamilies.size() > 0){
            for(Map.Entry<String, List<String>> entry : columnFamilies.entrySet()){
                List<String> columns = entry.getValue();
                if(columns == null){
                    scanVo.addFamily(Bytes.toBytes(entry.getKey()));
                } else {
                    for(String column : columns){
                        scanVo.addColumn(Bytes.toBytes(entry.getKey()), Bytes.toBytes(column));
                    }
                }
            }
        }

        return scanVo;
    }

    private static void saveData(Connection connection, String tablenName, Map<String, Map<String, Map<String, String>>> columns){
        List<Put> puts = new ArrayList<>();
        try(Table table = connection.getTable(TableName.valueOf(tablenName))) {
            for(Map.Entry<String,Map<String, Map<String, String>>> row : columns.entrySet()){
                String rowId = row.getKey();
                Put put = new Put(Bytes.toBytes(rowId));
                for(Map.Entry<String, Map<String,String>> columnFamily : row.getValue().entrySet()){
                    for(Map.Entry<String, String> entry : columnFamily.getValue().entrySet()){
                        put.addColumn(Bytes.toBytes(columnFamily.getKey()), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                    }

                }
                puts.add(put);

            }
            table.put(puts);
            table.close();
        } catch (IOException exception) {
            exception.printStackTrace();
        }
   }

   public static void deleteData(Connection connection,String tableName,  List<String> rowIds) throws IOException {
        for(String rowId : rowIds){
            Delete delete = new Delete(Bytes.toBytes(rowId));
            connection.getTable(TableName.valueOf(tableName)).delete(delete);
        }




   }



}
