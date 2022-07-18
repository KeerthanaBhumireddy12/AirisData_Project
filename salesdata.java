// ORM class for table 'salesdata'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Mon Jul 18 09:57:10 IST 2022
// For connector: org.apache.sqoop.manager.MySQLManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class salesdata extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  protected ResultSet __cur_result_set;
  private Integer region_id;
  public Integer get_region_id() {
    return region_id;
  }
  public void set_region_id(Integer region_id) {
    this.region_id = region_id;
  }
  public salesdata with_region_id(Integer region_id) {
    this.region_id = region_id;
    return this;
  }
  private String product_category;
  public String get_product_category() {
    return product_category;
  }
  public void set_product_category(String product_category) {
    this.product_category = product_category;
  }
  public salesdata with_product_category(String product_category) {
    this.product_category = product_category;
    return this;
  }
  private String order_date;
  public String get_order_date() {
    return order_date;
  }
  public void set_order_date(String order_date) {
    this.order_date = order_date;
  }
  public salesdata with_order_date(String order_date) {
    this.order_date = order_date;
    return this;
  }
  private String order_id;
  public String get_order_id() {
    return order_id;
  }
  public void set_order_id(String order_id) {
    this.order_id = order_id;
  }
  public salesdata with_order_id(String order_id) {
    this.order_id = order_id;
    return this;
  }
  private Integer amount;
  public Integer get_amount() {
    return amount;
  }
  public void set_amount(Integer amount) {
    this.amount = amount;
  }
  public salesdata with_amount(Integer amount) {
    this.amount = amount;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof salesdata)) {
      return false;
    }
    salesdata that = (salesdata) o;
    boolean equal = true;
    equal = equal && (this.region_id == null ? that.region_id == null : this.region_id.equals(that.region_id));
    equal = equal && (this.product_category == null ? that.product_category == null : this.product_category.equals(that.product_category));
    equal = equal && (this.order_date == null ? that.order_date == null : this.order_date.equals(that.order_date));
    equal = equal && (this.order_id == null ? that.order_id == null : this.order_id.equals(that.order_id));
    equal = equal && (this.amount == null ? that.amount == null : this.amount.equals(that.amount));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof salesdata)) {
      return false;
    }
    salesdata that = (salesdata) o;
    boolean equal = true;
    equal = equal && (this.region_id == null ? that.region_id == null : this.region_id.equals(that.region_id));
    equal = equal && (this.product_category == null ? that.product_category == null : this.product_category.equals(that.product_category));
    equal = equal && (this.order_date == null ? that.order_date == null : this.order_date.equals(that.order_date));
    equal = equal && (this.order_id == null ? that.order_id == null : this.order_id.equals(that.order_id));
    equal = equal && (this.amount == null ? that.amount == null : this.amount.equals(that.amount));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.region_id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.product_category = JdbcWritableBridge.readString(2, __dbResults);
    this.order_date = JdbcWritableBridge.readString(3, __dbResults);
    this.order_id = JdbcWritableBridge.readString(4, __dbResults);
    this.amount = JdbcWritableBridge.readInteger(5, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.region_id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.product_category = JdbcWritableBridge.readString(2, __dbResults);
    this.order_date = JdbcWritableBridge.readString(3, __dbResults);
    this.order_id = JdbcWritableBridge.readString(4, __dbResults);
    this.amount = JdbcWritableBridge.readInteger(5, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(region_id, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(product_category, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(order_date, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(order_id, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(amount, 5 + __off, 4, __dbStmt);
    return 5;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(region_id, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(product_category, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(order_date, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(order_id, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(amount, 5 + __off, 4, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.region_id = null;
    } else {
    this.region_id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.product_category = null;
    } else {
    this.product_category = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.order_date = null;
    } else {
    this.order_date = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.order_id = null;
    } else {
    this.order_id = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.amount = null;
    } else {
    this.amount = Integer.valueOf(__dataIn.readInt());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.region_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.region_id);
    }
    if (null == this.product_category) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, product_category);
    }
    if (null == this.order_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, order_date);
    }
    if (null == this.order_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, order_id);
    }
    if (null == this.amount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.amount);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.region_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.region_id);
    }
    if (null == this.product_category) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, product_category);
    }
    if (null == this.order_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, order_date);
    }
    if (null == this.order_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, order_id);
    }
    if (null == this.amount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.amount);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(region_id==null?"null":"" + region_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(product_category==null?"null":product_category, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(order_date==null?"null":order_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(order_id==null?"null":order_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(amount==null?"null":"" + amount, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(region_id==null?"null":"" + region_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(product_category==null?"null":product_category, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(order_date==null?"null":order_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(order_id==null?"null":order_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(amount==null?"null":"" + amount, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.region_id = null; } else {
      this.region_id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.product_category = null; } else {
      this.product_category = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.order_date = null; } else {
      this.order_date = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.order_id = null; } else {
      this.order_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.amount = null; } else {
      this.amount = Integer.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.region_id = null; } else {
      this.region_id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.product_category = null; } else {
      this.product_category = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.order_date = null; } else {
      this.order_date = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.order_id = null; } else {
      this.order_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.amount = null; } else {
      this.amount = Integer.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    salesdata o = (salesdata) super.clone();
    return o;
  }

  public void clone0(salesdata o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
    __sqoop$field_map.put("region_id", this.region_id);
    __sqoop$field_map.put("product_category", this.product_category);
    __sqoop$field_map.put("order_date", this.order_date);
    __sqoop$field_map.put("order_id", this.order_id);
    __sqoop$field_map.put("amount", this.amount);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("region_id", this.region_id);
    __sqoop$field_map.put("product_category", this.product_category);
    __sqoop$field_map.put("order_date", this.order_date);
    __sqoop$field_map.put("order_id", this.order_id);
    __sqoop$field_map.put("amount", this.amount);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if ("region_id".equals(__fieldName)) {
      this.region_id = (Integer) __fieldVal;
    }
    else    if ("product_category".equals(__fieldName)) {
      this.product_category = (String) __fieldVal;
    }
    else    if ("order_date".equals(__fieldName)) {
      this.order_date = (String) __fieldVal;
    }
    else    if ("order_id".equals(__fieldName)) {
      this.order_id = (String) __fieldVal;
    }
    else    if ("amount".equals(__fieldName)) {
      this.amount = (Integer) __fieldVal;
    }
    else {
      throw new RuntimeException("No such field: " + __fieldName);
    }
  }
  public boolean setField0(String __fieldName, Object __fieldVal) {
    if ("region_id".equals(__fieldName)) {
      this.region_id = (Integer) __fieldVal;
      return true;
    }
    else    if ("product_category".equals(__fieldName)) {
      this.product_category = (String) __fieldVal;
      return true;
    }
    else    if ("order_date".equals(__fieldName)) {
      this.order_date = (String) __fieldVal;
      return true;
    }
    else    if ("order_id".equals(__fieldName)) {
      this.order_id = (String) __fieldVal;
      return true;
    }
    else    if ("amount".equals(__fieldName)) {
      this.amount = (Integer) __fieldVal;
      return true;
    }
    else {
      return false;    }
  }
}
