package org.pengtao.flume.sink;

/**
 * Created by yangyibo on 17/1/5.
 */

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.IntStream;

public class MysqlSink extends AbstractSink implements Configurable {

    private Logger LOG = LoggerFactory.getLogger(MysqlSink.class);
    private String hostname;
    private String port;
    private String databaseName;
    private String tableName;
    private String user;
    private String password;
    private PreparedStatement preparedStatement;
    private Connection conn;
    private int batchSize;
//    private String url;
    private String columns;
    private String sql;
    private String[] paras_list;

    public MysqlSink() {
        LOG.info("MysqlSink start...");
    }

    public void configure(Context context) {
        hostname = context.getString("hostname");
        Preconditions.checkNotNull(hostname, "hostname must be set!!");
        port = context.getString("port");
        Preconditions.checkNotNull(port, "port must be set!!");
        databaseName = context.getString("databaseName");
        Preconditions.checkNotNull(databaseName, "databaseName must be set!!");
        tableName = context.getString("tableName");
        Preconditions.checkNotNull(tableName, "tableName must be set!!");
        user = context.getString("user");
        Preconditions.checkNotNull(user, "user must be set!!");

//        password = new String(Base64.getDecoder().decode(context.getString("password")));
//        password = password.substring(0, password.indexOf(","));

        password = context.getString("password");
        Preconditions.checkNotNull(password, "password must be set!!");
        batchSize = context.getInteger("batchSize", 100);
        Preconditions.checkNotNull(batchSize > 0, "batchSize must be a positive number!!");
//        url = context.getString("url");
//        Preconditions.checkNotNull(url, "hostname must be url!!");
        columns = context.getString("columns");
        Preconditions.checkNotNull(columns, "columns must be set!!");
    }

    @Override
    public void start() {
        super.start();
        try {
            //调用Class.forName()方法加载驱动程序
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        String url = "jdbc:mysql://" + hostname + ":" + port + "/" + databaseName +
                "?useUnicode=true&characterEncoding=UTF-8";
        //调用DriverManager对象的getConnection()方法，获得一个Connection对象

        try {
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);
            //创建一个Statement对象
//            preparedStatement = conn.prepareStatement("insert into " + tableName +
//                    " (content,status,create_time) values  (?,0,now())");
            paras_list = columns.split(",");
            IntStream.range(0, paras_list.length).forEach(i -> paras_list[i] = "?");
            String parameters = Arrays.toString(paras_list).replace("[" ,"").replace("]","");
            sql = String.format("insert into %s(%s) values (%s)",tableName,columns,parameters);
            LOG.info(sql);
            preparedStatement = conn.prepareStatement(sql);

        } catch (SQLException e) {
            e.printStackTrace();
//            System.exit(1);
        }

    }

    @Override
    public void stop() {
        super.stop();
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event;
        String content;
        List<String> contents = new ArrayList<>();
        String[] values;
        transaction.begin();
        try {
            for (int i = 0; i < batchSize; i++) {
                event = channel.take();
                if (event != null) {//对事件进行处理
                    //event 的 body 为   "exec tail$i , abel"
                    content = new String(event.getBody());
                    if (content.split(",").length != paras_list.length) {
                        LOG.error(String.format("'%s' separator number has not enough,skip this one", content));
                        continue;
                    }
                    contents.add(content);

                } else {
                    result = Status.BACKOFF;
                    break;
                }
            }

            if (contents.size() > 0) {
                preparedStatement.clearBatch();
                for (String temp : contents) {
                    values = temp.split(",");
                    for (int i = 1; i <= values.length; i++) {
                        preparedStatement.setString(i, values[i - 1]);
                    }
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();

                conn.commit();

            }
            transaction.commit();
        } catch (Exception e) {
            try {
                transaction.rollback();
            } catch (Exception e2) {
                LOG.error("Exception in rollback. Rollback might not have been" +
                        "successful.", e2);
            }
            LOG.error("Failed to commit transaction." +
                    "Transaction rolled back.", e);
            Throwables.propagate(e);
        } finally {
            transaction.close();
//            try {
//                if (contents.size() > 0) {
//                    for (String temp : contents) {
//                        HttpRequest.sendPost(url, temp);
//                    }
//                }
//            } catch (Exception ex) {
//
//            }
        }

        return result;
    }
}