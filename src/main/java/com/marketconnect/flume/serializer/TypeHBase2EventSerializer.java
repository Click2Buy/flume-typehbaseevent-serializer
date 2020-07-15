package com.marketconnect.flume.serializer;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase2.HBase2EventSerializer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.collect.Lists;

public class TypeHBase2EventSerializer implements HBase2EventSerializer {
  // Config vars
  /** Type used to send data to hbase. */
  public static final String TYPES_CONFIG = "types";
  public static final String TYPES_DEFAULT = "string";

  /** Comma separated list of column names to place matching groups in. */
  public static final String COL_NAME_CONFIG = "colNames";
  public static final String COLUMN_NAME_DEFAULT = "col";

  /** What charset to use when serializing into HBase's byte arrays */
  public static final String CHARSET_CONFIG = "charset";
  public static final String CHARSET_DEFAULT = "UTF-8";

  protected byte[] cf;
  private byte[] payload;
  private Map<String, String> colNames;
  private Map<String, String> headers;
  private Charset charset;

  @Override
  public void configure(Context context) {
    charset = Charset.forName(context.getString(CHARSET_CONFIG,
        CHARSET_DEFAULT));

    String typesStr = context.getString(TYPES_CONFIG, TYPES_DEFAULT);
    String[] types = typesStr.split(",");

    String colNameStr = context.getString(COL_NAME_CONFIG, COLUMN_NAME_DEFAULT);
    String[] columnNames = colNameStr.split(",");

    this.colNames = Maps.newHashMap();
    for (int i = 0; i < columnNames.length; i++) {
        String c = columnNames[i];
        String t = types[i];
        this.colNames.put(c, t);
    }
  }

  @Override
  public void configure(ComponentConfiguration conf) {
  }

  @Override
  public void initialize(Event event, byte[] columnFamily) {
    this.headers = event.getHeaders();
    this.payload = event.getBody();
    this.cf = columnFamily;
  }

  @Override
  public List<Row> getActions() throws FlumeException {
    List<Row> actions = Lists.newArrayList();
    byte[] rowKey = this.payload;

    if (rowKey.length == 0) {
      return Lists.newArrayList();
    }

    try {
      Put put = new Put(rowKey);

      for (Map.Entry<String, String> entry : headers.entrySet()) {
          String entryStr = entry.getKey();
          String valueStr = entry.getValue();
          if (valueStr == null) continue;
          String type = colNames.get(entryStr);
          try {
              if ("string".equalsIgnoreCase(type)) {
                  put.addColumn(cf, entryStr.getBytes(charset), Bytes.toBytes(valueStr));
              } else if ("double".equalsIgnoreCase(type)) {
                  put.addColumn(cf, entryStr.getBytes(charset), Bytes.toBytes(Double.parseDouble(valueStr)));
              } else if ("float".equalsIgnoreCase(type)) {
                  put.addColumn(cf, entryStr.getBytes(charset), Bytes.toBytes(Float.parseFloat(valueStr)));
              } else if ("int".equalsIgnoreCase(type)) {
                  put.addColumn(cf, entryStr.getBytes(charset), Bytes.toBytes(Integer.parseInt(valueStr)));
              } else if ("long".equalsIgnoreCase(type)) {
                  put.addColumn(cf, entryStr.getBytes(charset), Bytes.toBytes(Long.parseLong(valueStr)));
              } else if ("short".equalsIgnoreCase(type)) {
                  put.addColumn(cf, entryStr.getBytes(charset), Bytes.toBytes(Short.parseShort(valueStr)));
              }
          } catch (Exception e) {
              StringWriter sw = new StringWriter();
              e.printStackTrace(new PrintWriter(sw));
              String exceptionAsString = sw.toString();
              throw new FlumeException(e.toString() + " row key " + Bytes.toString(rowKey) + " entryStr " + entryStr + "valueStr " + valueStr + " type " + type + " " + exceptionAsString);
          }
      }
      actions.add(put);
    } catch (Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String exceptionAsString = sw.toString();
        throw new FlumeException(e.toString() + " row key " + Bytes.toString(rowKey) + exceptionAsString);
    }
    return actions;
  }

  @Override
  public List<Increment> getIncrements() {
    return Lists.newArrayList();
  }

  @Override
  public void close() {  }
}
