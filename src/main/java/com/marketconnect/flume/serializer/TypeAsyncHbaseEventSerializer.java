package com.marketconnect.flume.serializer;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.AsyncHbaseEventSerializer;
import org.apache.hadoop.hbase.util.Bytes;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.collect.Lists;

public class TypeAsyncHbaseEventSerializer implements AsyncHbaseEventSerializer {
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

  private byte[] table;
  private byte[] cf;
  private byte[] payload;
  private Map<String, String> colNames;
  private Map<String, String> headers;
  private Charset charset;

  @Override
  public void initialize(byte[] table, byte[] cf) {
    this.table = table;
    this.cf = cf;
  }

  @Override
  public List<PutRequest> getActions() throws FlumeException {
    List<PutRequest> actions = Lists.newArrayList();
    byte[] rowKey = this.payload;

    if (rowKey.length == 0) {
      return Lists.newArrayList();
    }

    try {
      List<byte[]> qualifiers = Lists.newArrayList();
      List<byte[]> values = Lists.newArrayList();

      for (Map.Entry<String, String> entry : headers.entrySet()) {
          String entryStr = entry.getKey();
          String valueStr = entry.getValue();
          String type = colNames.get(entryStr);
          if ("string".equalsIgnoreCase(type)) {
              qualifiers.add(entryStr.getBytes(charset));
              values.add(Bytes.toBytes(valueStr));
          } else if ("double".equalsIgnoreCase(type)) {
              qualifiers.add(entryStr.getBytes(charset));
              values.add(Bytes.toBytes(Double.parseDouble(valueStr)));
          } else if ("float".equalsIgnoreCase(type)) {
              qualifiers.add(entryStr.getBytes(charset));
              values.add(Bytes.toBytes(Float.parseFloat(valueStr)));
          } else if ("int".equalsIgnoreCase(type)) {
              qualifiers.add(entryStr.getBytes(charset));
              values.add(Bytes.toBytes(Integer.parseInt(valueStr)));
          } else if ("long".equalsIgnoreCase(type)) {
              qualifiers.add(entryStr.getBytes(charset));
              values.add(Bytes.toBytes(Long.parseLong(valueStr)));
          } else if ("short".equalsIgnoreCase(type)) {
              qualifiers.add(entryStr.getBytes(charset));
              values.add(Bytes.toBytes(Short.parseShort(valueStr)));
          }
      }
      PutRequest putRequest =  new PutRequest(table, rowKey, cf,
                                              qualifiers.toArray(new byte[qualifiers.size()][]), values.toArray(new byte[values.size()][]));
      actions.add(putRequest);
    } catch (IllegalArgumentException e) {
      throw new FlumeException(e + " row key " + Bytes.toString(rowKey));
    } catch (Exception e) {
      throw new FlumeException(e);
    }
    return actions;
  }

  @Override
  public List<AtomicIncrementRequest> getIncrements() {
    return Lists.newArrayList();
  }

  @Override
  public void cleanUp() {
    // TODO Auto-generated method stub

  }

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
  public void setEvent(Event event) {
    this.headers = event.getHeaders();
    this.payload = event.getBody();
  }

  @Override
  public void configure(ComponentConfiguration conf) {
  }
}
