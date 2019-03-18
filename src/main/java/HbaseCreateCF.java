import org.apache.hadoop.hbase.io.compress.Compression;

import java.util.List;
import java.util.Map;

/**
 * @author yangzheng10
 * @version $Id: HbaseCreateCF.java, v 0.1 2019年03月15日 14:56:27 yangzheng10 Exp $
 *
 * desc: hbase创建表封装的ColumnFamily
 */
public class HbaseCreateCF {
    /**
     * key: ColumnFamily名字
     * value:压缩格式,每个ColumnFamily都可以设置不同的压缩格式，或者只压缩部分ColumnFamily
     */
    private List<Map<String, Compression.Algorithm>> list;

    public List<Map<String, Compression.Algorithm>> getList() {
        return list;
    }

    public void setList(List<Map<String, Compression.Algorithm>> list) {
        this.list = list;
    }

    public HbaseCreateCF(List<Map<String, Compression.Algorithm>> list) {
        super();
        this.list = list;
    }

    public HbaseCreateCF() {
        super();
        // TODO Auto-generated constructor stub
    }

    @Override
    public String toString() {
        return "HbaseCreateCF [list=" + list + "]";
    }
}
