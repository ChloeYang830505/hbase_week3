package util;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author YangChunping
 * @version 1.0
 * @date 2022/3/14 12:40
 * @description
 */

public class HbaseUtil {
    private Connection connection;

    public HbaseUtil(Connection connection){
        this.connection = connection;
    }
    public boolean createTable(String tableName, List<String> columnFaimily){
         try(Admin adminHbase =connection.getAdmin()) {
             TableName name = TableName.valueOf(tableName);
             boolean exists = adminHbase.tableExists(name);
             if(exists){
                 return true;
             }
             TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(name);
             for(String cf : columnFaimily){
                 ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).setCompressTags(true).setCompressionType(Compression.Algorithm.ZSTD)
                         .setMaxVersions(1).build();
                 tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
             }
             tableDescriptorBuilder.setDurability(Durability.SYNC_WAL);
             tableDescriptorBuilder.setCompactionEnabled(true);

             adminHbase.createTable(tableDescriptorBuilder.build());
             return true;
         }
         catch (IOException e){
             System.out.println("exception : " + e);
         }
         return false;
    }

    public boolean dropTable(String tableName){
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

    public void getData(String tableName, String startRowKey, String endRowKey, Map<String, List<String>> columnFamilies){
        try(Admin adminHbase = connection.getAdmin()){
            TableName table = TableName.valueOf(tableName);
            if(adminHbase.tableExists(table)){
                Table hbaseTable = connection.getTable(table);
                List<Result> result = scanRows(hbaseTable, startRowKey, endRowKey,columnFamilies);
            }
        } catch (IOException exception){
            System.out.println(exception);
        }

    }

    private List<Result> scanRows(Table table, String startRowKey, String endRowKey, Map<String, List<String>> columnFamilies) throws IOException {
        List<Result> result = new ArrayList<>();
        Scan scanVo = buildScan(startRowKey, endRowKey, columnFamilies);
        ResultScanner scanResult = table.getScanner(scanVo);
        for(Result row : scanResult){
            result.add(row);
        }
        return result;
    }

    private Scan buildScan(String startRowKey, String endRowKey, Map<String, List<String>> columnFamilies) {
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
}
