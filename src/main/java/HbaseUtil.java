import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

/**
 * @author yangzheng10
 * @version $Id: HbaseUtil.java, v 0.1 2019年03月15日 10:50:01 yangzheng10 Exp $
 * from: https://blog.csdn.net/qq_20641565/article/details/56494428
 */
public class HbaseUtil {
    /**
     * 日志
     */
    private static final Logger logger = LoggerFactory.getLogger(HbaseUtil.class);

    /**
     * hbase操作对象
     */
    private static Admin hBaseAdmin;

    /**
     * 配置
     */
    private static Configuration conf = null;

    /**
     * hbase连接
     */
    private static Connection connection = null;

    /**
     * 初始化hbaseAdmin
     */
    static {
        Properties pro = null;

        try {
            pro = new Properties();
            String path = HbaseUtil.class.getResource("/").getFile().toString() + "hbase.properties";
            FileInputStream fis = new FileInputStream(new File(path));
            pro.load(fis);
            String HBASE_MASTER = pro.getProperty("hbase.master");
            String HBASE_ZK_CONNECT = pro.getProperty("hbase.zookeeper.quorum");
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.zookeeper.quorum", HBASE_ZK_CONNECT);
            conf.set("hbase.master", HBASE_MASTER);
            connection = ConnectionFactory.createConnection(conf);
            hBaseAdmin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * main
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        // 1.测试创建表,列族为cf1和cf2都用snappy压缩,每个类簇可以设置不同的压缩格式
//        HbaseCreateCF hcc = new HbaseCreateCF();
//        List<Map<String, Compression.Algorithm>> list = new ArrayList<Map<String, Compression.Algorithm>>();
//        Map<String, Compression.Algorithm> map1 = new HashMap<String, Compression.Algorithm>();
//        Map<String, Compression.Algorithm> map2 = new HashMap<String, Compression.Algorithm>();
//        map1.put("cf1", Compression.Algorithm.GZ);
//        map2.put("cf2", Compression.Algorithm.NONE);
//        list.add(map1);
//        list.add(map2);
//        hcc.setList(list);
//        HbaseUtil hu = new HbaseUtil();
//        hu.createTable("test01", hcc, null, false);

        // 2.测试表是否创建成功
//         System.out.println(isExist("test01"));

        // 3.测试同步插入数据
//        List<Put> puts = new ArrayList<Put>();
//        for(int i = 0; i < 20; i++) {
//            // 创建put对象 ，一个put操作一行数据，并设置rowkey名称
//            Put put = new Put(Bytes.toBytes("00" + i));
//            put.addColumn("cf1".getBytes(), Bytes.toBytes("name1"), Bytes.toBytes("name1_" + i));
//            put.addColumn("cf1".getBytes(), Bytes.toBytes("age1"), Bytes.toBytes(Integer.toString(i)));
//            put.addColumn("cf2".getBytes(), Bytes.toBytes("name2"), Bytes.toBytes("name2_" + i));
//            put.addColumn("cf2".getBytes(), Bytes.toBytes("age2"), Bytes.toBytes(Integer.toString(i + 100)));
//            puts.add(put);
//        }
//
//        addDataBatch("test01", puts);


        // 4.测试打印一条记录
//        Result res = getRow("test01", "000");
//        showOneRecordByOneRes(res);


        // 5.测试打印多条记录
//        List<String> rows = new ArrayList<>();
//        for (int i = 0; i < 20; i++) {
//            rows.add("00" + i);
//        }
//        Result[] results = getRows("test01", rows);
//        showOneRecordByRes(results);


        // 6.测试异步插入数据
//        List<Put> puts = new ArrayList<Put>();
//        for(int i = 20; i < 40; i++) {
//            // 创建put对象 ，一个put操作一行数据，并设置rowkey名称
//            Put put = new Put(Bytes.toBytes("00" + i));
//            put.addColumn("cf1".getBytes(), Bytes.toBytes("name1"), Bytes.toBytes("name1_" + i));
//            put.addColumn("cf1".getBytes(), Bytes.toBytes("age1"), Bytes.toBytes(Integer.toString(i)));
//            put.addColumn("cf2".getBytes(), Bytes.toBytes("name2"), Bytes.toBytes("name2_" + i));
//            put.addColumn("cf2".getBytes(), Bytes.toBytes("age2"), Bytes.toBytes(Integer.toString(i + 100)));
//            puts.add(put);
//        }
//
//        addDataBatchAsyn("test01", puts);

        // 7.测试打印多条记录
//        List<String> rows = new ArrayList<>();
//        for (int i = 20; i < 40; i++) {
//            rows.add("00" + i);
//        }
//        Result[] results = getRows("test01", rows);
//        showOneRecordByRes(results);

        // 8.测试删除
//        delete("test01", "000");
//        Result res = getRow("test01", "000");
//        showOneRecordByOneRes(res);
    }


    /**
     * 根据多个Result数据打印多条记录
     * @param res
     */
    public static void showOneRecordByRes(Result[] res) {
        for (Result r : res) {
            showOneRecordByOneRes(r);
            System.out.println("-----------------------");
        }
    }

    /**
     * 根据一个Result数据打印一条记录
     * @param r
     */
    public static void showOneRecordByOneRes(Result r) {
        System.out.println("rowkey: " + Bytes.toString(r.getRow()));

        for (Cell c : r.rawCells()) {
            String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:MM:ss").format(new Date(c.getTimestamp()));
            System.out.println("[" + timestamp + "] " +
                    Bytes.toString(CellUtil.cloneFamily(c)) + ":" +
                    Bytes.toString(CellUtil.cloneQualifier(c)) + ":" +
                    Bytes.toString(CellUtil.cloneValue(c)));
        }
    }


    /**
     * 判断表是否存在
     * @param tableName 表名
     * @return
     * @throws IOException
     */
    public static boolean isExist(String tableName) throws IOException {
        // 封装表对象
        TableName tn = TableName.valueOf(tableName);
        return hBaseAdmin.tableExists(tn);
    }

    /**
     * 根据自定义region个数，以及region的startKey，endKey 创建表
     *
     * @param tableName
     *            表名
     * @param hcc
     *            列族列表
     * @param splits
     *            自定义region
     * @param isDelete
     *            表存在是否删除
     * @throws IOException
     */
    public void createTable(String tableName, HbaseCreateCF hcc,
                            byte[][] splits, boolean isDelete) throws Exception {

        // 封装表对象
        TableName tn = TableName.valueOf(tableName);
        try {
            // 判断表是否存在
            if (hBaseAdmin.tableExists(tn)) {
                if (isDelete) {

                    // disable表
                    hBaseAdmin.disableTable(tn);

                    // 删除表
                    hBaseAdmin.deleteTable(tn);

                    System.out.println("表名为:" + tableName + "在建表时，被删除！");
                } else {
                    System.out.println("表名为:" + tableName + "已经存在！");
                    throw new Exception("isDelete为false，表名已经存在！");
                }
            }

            // 创建表描述对象
            HTableDescriptor htd = new HTableDescriptor(tn);

            // 获取list
            List<Map<String, Algorithm>> list = hcc.getList();

            // 遍历并且往表描述对象添加列族描述对象
            for (Map<String, Algorithm> map : list) {

                // 创建ColumFamily描述对象
                Set<Entry<String, Algorithm>> entrySet = map.entrySet();

                // 取出map的内容，key为cf名，value为压缩类型
                Entry<String, Algorithm> next = entrySet.iterator().next();

                // 取出列族
                String columnFamily = next.getKey();

                // 取出压缩格式
                Algorithm algorithm = next.getValue();

                // 创建列族描述对象
                HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);

                // 压缩是否为空
                if (null != algorithm) {

                    // 添加列族并且设置压缩
                    //setCompressionType最后文件的压缩格式
                    //setCompactionCompressionType是StoreFile合并的时候压缩格式
                    htd.addFamily(hcd.setCompressionType(algorithm)
                            .setCompactionCompressionType(algorithm));
                } else {

                    // 添加列族不压缩
                    htd.addFamily(hcd);
                }

            }

            // 是否预分区
            if (splits == null) {

                // 默认一个region
                hBaseAdmin.createTable(htd);
            } else {

                //
                hBaseAdmin.createTable(htd, splits);
            }
        } finally {

            closeConnect(null, null, hBaseAdmin, null, null);
        }

    }

    /**
     * 异步批量写入数据
     *
     * @param tableName
     * @param puts
     * @throws Exception
     */
    public static void addDataBatchAsyn(String tableName, List<Put> puts)
            throws Exception {
        long currentTime = System.currentTimeMillis();

        // 创建监听器
        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e,
                                    BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    logger.error("写入失败： " + e.getRow(i) + "！");
                }
            }
        };

        // 设置表的缓存参数
        BufferedMutatorParams params = new BufferedMutatorParams(
                TableName.valueOf(tableName)).listener(listener);
        params.writeBufferSize(5 * 1024 * 1024);

        // 操作类获取
        final BufferedMutator mutator = connection.getBufferedMutator(params);
        try {

            // 添加
            mutator.mutate(puts);

            // 提交
            mutator.flush();
        } finally {

            // 关闭连接
            closeConnect(connection, mutator, null, null, null);
        }
        logger.info("addDataBatchAsyn执行时间:{}",
                (System.currentTimeMillis() - currentTime));
    }

    /**
     * 同步批量添加数据
     *
     * @param tablename
     * @param puts
     * @return
     * @throws Exception
     */
    public static void addDataBatch(String tablename, List<?> puts)
            throws Exception {
        long currentTime = System.currentTimeMillis();

        // 获取htable操作对象
        HTable htable = (HTable) connection.getTable(TableName
                .valueOf(tablename));

        // 这里设置了false,setWriteBufferSize方法才能生效
        htable.setAutoFlushTo(false);
        htable.setWriteBufferSize(5 * 1024 * 1024);

        try {

            // 添加
            htable.put((List<Put>) puts);

            // 执行
            htable.flushCommits();
        } finally {

            // 关闭连接
            closeConnect(connection, null, null, htable, null);
        }

        logger.info("addDataBatch执行时间:{}",
                (System.currentTimeMillis() - currentTime));
    }

    /**
     * 删除单条数据
     *
     * @param tablename
     * @param row
     * @throws IOException
     */
    public static void delete(String tablename, String row) throws IOException {

        // 获取htable操作对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        if (table != null) {
            try {

                // 创建删除对象
                Delete d = new Delete(row.getBytes());

                // 执行删除操作
                table.delete(d);
            } finally {

                // 关闭连接
                closeConnect(null, null, null, null, table);
            }
        }
    }

    /**
     * 删除多行数据
     *
     * @param tablename
     * @param rows
     * @throws IOException
     */
    public static void delete(String tablename, String[] rows)
            throws IOException {

        // 获取htable操作对象
        Table table = connection.getTable(TableName.valueOf(tablename));
        if (table != null) {
            try {

                // 存储删除对象List
                List<Delete> list = new ArrayList<Delete>();
                for (String row : rows) {

                    // 创建删除对象
                    Delete d = new Delete(row.getBytes());

                    // 添加到删除对象List中
                    list.add(d);
                }
                if (list.size() > 0) {

                    // 执行删除操作
                    table.delete(list);
                }
            } finally {

                // 关闭连接
                closeConnect(null, null, null, null, table);
            }
        }
    }

    /**
     * 获取单条数据,根据rowkey
     *
     * @param tablename
     * @param row
     * @return
     * @throws Exception
     */
    public static Result getRow(String tablename, String row) throws Exception {

        // 获取htable操作对象
        Table table = connection.getTable(TableName.valueOf(tablename));
        Result rs = null;
        if (table != null) {
            try {

                // 创建查询对象
                Get g = new Get(row.getBytes());

                // 查询获得结果
                rs = table.get(g);
            } catch (IOException e) {
                logger.error("获取失败！", e);
            } finally {
                // 关闭连接
                closeConnect(null, null, null, null, table);
            }
        }
        return rs;
    }

    /**
     * 获取多行数据
     *
     * @param tablename
     * @param rows
     * @return
     * @throws Exception
     */
    public static Result[] getRows(String tablename, List<String> rows)
            throws Exception {
        // 获取htable操作对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        List<Get> gets = null;
        Result[] results = null;

        try {
            if (table != null) {

                // 创建查询操作的List
                gets = new ArrayList<Get>();
                for (String row : rows) {
                    if (row != null) {

                        // 封装到查询操作list中
                        gets.add(new Get(Bytes.toBytes(row)));
                    } else {
                        throw new RuntimeException("rows 没有数据！");
                    }
                }
            }
            if (gets.size() > 0) {

                // 查询数据
                results = table.get(gets);
            }
        } catch (IOException e) {
            logger.error("获取数据失败！", e);
        } finally {

            // 关闭连接
            closeConnect(null, null, null, null, table);
        }
        return results;
    }

    /**
     * 关闭连接
     *
     * @param conn
     * @param mutator
     * @param admin
     * @param htable
     */
    public static void closeConnect(Connection conn, BufferedMutator mutator,
                                    Admin admin, HTable htable, Table table) {
        if (null != conn) {
            try {
                conn.close();
            } catch (Exception e) {
                logger.error("closeConnect failure !", e);
            }
        }

        if (null != mutator) {
            try {
                mutator.close();
            } catch (Exception e) {
                logger.error("closeBufferedMutator failure !", e);
            }
        }

        if (null != admin) {
            try {
                admin.close();
            } catch (Exception e) {
                logger.error("closeAdmin failure !", e);
            }
        }
        if (null != htable) {
            try {
                htable.close();
            } catch (Exception e) {
                logger.error("closeHtable failure !", e);
            }
        }
        if (null != table) {
            try {
                table.close();
            } catch (Exception e) {
                logger.error("closeTable failure !", e);
            }
        }
    }
}