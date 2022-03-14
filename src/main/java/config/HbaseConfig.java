package config;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import util.HbaseUtil;

import java.io.IOException;

/**
 * @author YangChunping
 * @version 1.0
 * @date 2022/3/14 15:07
 * @description
 */
@Configuration
public class HbaseConfig {

    @Bean
    public Connection hbaseConnection() throws IOException {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        return ConnectionFactory.createConnection(configuration);
    }

    @Bean
    public HbaseUtil createHbaseUtil(Connection connection){
        return new HbaseUtil(connection);
    }
}
