/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hbase.thrift.generated;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A PartitionScan object is used to specify scanner parameters when opening a scanner.
 */
public class TMessageScan implements org.apache.thrift.TBase<TMessageScan, TMessageScan._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TMessageScan");

  private static final org.apache.thrift.protocol.TField START_MESSAGE_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("startMessageID", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField STOP_MESSAGE_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("stopMessageID", org.apache.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.thrift.protocol.TField TOPICS_FIELD_DESC = new org.apache.thrift.protocol.TField("topics", org.apache.thrift.protocol.TType.LIST, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TMessageScanStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TMessageScanTupleSchemeFactory());
  }

  public TMessageID startMessageID; // required
  public TMessageID stopMessageID; // required
  public List<ByteBuffer> topics; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    START_MESSAGE_ID((short)1, "startMessageID"),
    STOP_MESSAGE_ID((short)2, "stopMessageID"),
    TOPICS((short)3, "topics");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // START_MESSAGE_ID
          return START_MESSAGE_ID;
        case 2: // STOP_MESSAGE_ID
          return STOP_MESSAGE_ID;
        case 3: // TOPICS
          return TOPICS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private _Fields optionals[] = {_Fields.TOPICS};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.START_MESSAGE_ID, new org.apache.thrift.meta_data.FieldMetaData("startMessageID", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TMessageID.class)));
    tmpMap.put(_Fields.STOP_MESSAGE_ID, new org.apache.thrift.meta_data.FieldMetaData("stopMessageID", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TMessageID.class)));
    tmpMap.put(_Fields.TOPICS, new org.apache.thrift.meta_data.FieldMetaData("topics", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING            , "Text"))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TMessageScan.class, metaDataMap);
  }

  public TMessageScan() {
  }

  public TMessageScan(
    TMessageID startMessageID,
    TMessageID stopMessageID)
  {
    this();
    this.startMessageID = startMessageID;
    this.stopMessageID = stopMessageID;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TMessageScan(TMessageScan other) {
    if (other.isSetStartMessageID()) {
      this.startMessageID = new TMessageID(other.startMessageID);
    }
    if (other.isSetStopMessageID()) {
      this.stopMessageID = new TMessageID(other.stopMessageID);
    }
    if (other.isSetTopics()) {
      List<ByteBuffer> __this__topics = new ArrayList<ByteBuffer>();
      for (ByteBuffer other_element : other.topics) {
        __this__topics.add(other_element);
      }
      this.topics = __this__topics;
    }
  }

  public TMessageScan deepCopy() {
    return new TMessageScan(this);
  }

  @Override
  public void clear() {
    this.startMessageID = null;
    this.stopMessageID = null;
    this.topics = null;
  }

  public TMessageID getStartMessageID() {
    return this.startMessageID;
  }

  public TMessageScan setStartMessageID(TMessageID startMessageID) {
    this.startMessageID = startMessageID;
    return this;
  }

  public void unsetStartMessageID() {
    this.startMessageID = null;
  }

  /** Returns true if field startMessageID is set (has been assigned a value) and false otherwise */
  public boolean isSetStartMessageID() {
    return this.startMessageID != null;
  }

  public void setStartMessageIDIsSet(boolean value) {
    if (!value) {
      this.startMessageID = null;
    }
  }

  public TMessageID getStopMessageID() {
    return this.stopMessageID;
  }

  public TMessageScan setStopMessageID(TMessageID stopMessageID) {
    this.stopMessageID = stopMessageID;
    return this;
  }

  public void unsetStopMessageID() {
    this.stopMessageID = null;
  }

  /** Returns true if field stopMessageID is set (has been assigned a value) and false otherwise */
  public boolean isSetStopMessageID() {
    return this.stopMessageID != null;
  }

  public void setStopMessageIDIsSet(boolean value) {
    if (!value) {
      this.stopMessageID = null;
    }
  }

  public int getTopicsSize() {
    return (this.topics == null) ? 0 : this.topics.size();
  }

  public java.util.Iterator<ByteBuffer> getTopicsIterator() {
    return (this.topics == null) ? null : this.topics.iterator();
  }

  public void addToTopics(ByteBuffer elem) {
    if (this.topics == null) {
      this.topics = new ArrayList<ByteBuffer>();
    }
    this.topics.add(elem);
  }

  public List<ByteBuffer> getTopics() {
    return this.topics;
  }

  public TMessageScan setTopics(List<ByteBuffer> topics) {
    this.topics = topics;
    return this;
  }

  public void unsetTopics() {
    this.topics = null;
  }

  /** Returns true if field topics is set (has been assigned a value) and false otherwise */
  public boolean isSetTopics() {
    return this.topics != null;
  }

  public void setTopicsIsSet(boolean value) {
    if (!value) {
      this.topics = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case START_MESSAGE_ID:
      if (value == null) {
        unsetStartMessageID();
      } else {
        setStartMessageID((TMessageID)value);
      }
      break;

    case STOP_MESSAGE_ID:
      if (value == null) {
        unsetStopMessageID();
      } else {
        setStopMessageID((TMessageID)value);
      }
      break;

    case TOPICS:
      if (value == null) {
        unsetTopics();
      } else {
        setTopics((List<ByteBuffer>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case START_MESSAGE_ID:
      return getStartMessageID();

    case STOP_MESSAGE_ID:
      return getStopMessageID();

    case TOPICS:
      return getTopics();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case START_MESSAGE_ID:
      return isSetStartMessageID();
    case STOP_MESSAGE_ID:
      return isSetStopMessageID();
    case TOPICS:
      return isSetTopics();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TMessageScan)
      return this.equals((TMessageScan)that);
    return false;
  }

  public boolean equals(TMessageScan that) {
    if (that == null)
      return false;

    boolean this_present_startMessageID = true && this.isSetStartMessageID();
    boolean that_present_startMessageID = true && that.isSetStartMessageID();
    if (this_present_startMessageID || that_present_startMessageID) {
      if (!(this_present_startMessageID && that_present_startMessageID))
        return false;
      if (!this.startMessageID.equals(that.startMessageID))
        return false;
    }

    boolean this_present_stopMessageID = true && this.isSetStopMessageID();
    boolean that_present_stopMessageID = true && that.isSetStopMessageID();
    if (this_present_stopMessageID || that_present_stopMessageID) {
      if (!(this_present_stopMessageID && that_present_stopMessageID))
        return false;
      if (!this.stopMessageID.equals(that.stopMessageID))
        return false;
    }

    boolean this_present_topics = true && this.isSetTopics();
    boolean that_present_topics = true && that.isSetTopics();
    if (this_present_topics || that_present_topics) {
      if (!(this_present_topics && that_present_topics))
        return false;
      if (!this.topics.equals(that.topics))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_startMessageID = true && (isSetStartMessageID());
    builder.append(present_startMessageID);
    if (present_startMessageID)
      builder.append(startMessageID);

    boolean present_stopMessageID = true && (isSetStopMessageID());
    builder.append(present_stopMessageID);
    if (present_stopMessageID)
      builder.append(stopMessageID);

    boolean present_topics = true && (isSetTopics());
    builder.append(present_topics);
    if (present_topics)
      builder.append(topics);

    return builder.toHashCode();
  }

  public int compareTo(TMessageScan other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    TMessageScan typedOther = (TMessageScan)other;

    lastComparison = Boolean.valueOf(isSetStartMessageID()).compareTo(typedOther.isSetStartMessageID());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStartMessageID()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.startMessageID, typedOther.startMessageID);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStopMessageID()).compareTo(typedOther.isSetStopMessageID());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStopMessageID()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.stopMessageID, typedOther.stopMessageID);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTopics()).compareTo(typedOther.isSetTopics());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTopics()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.topics, typedOther.topics);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TMessageScan(");
    boolean first = true;

    sb.append("startMessageID:");
    if (this.startMessageID == null) {
      sb.append("null");
    } else {
      sb.append(this.startMessageID);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("stopMessageID:");
    if (this.stopMessageID == null) {
      sb.append("null");
    } else {
      sb.append(this.stopMessageID);
    }
    first = false;
    if (isSetTopics()) {
      if (!first) sb.append(", ");
      sb.append("topics:");
      if (this.topics == null) {
        sb.append("null");
      } else {
        sb.append(this.topics);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
    if (startMessageID != null) {
      startMessageID.validate();
    }
    if (stopMessageID != null) {
      stopMessageID.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TMessageScanStandardSchemeFactory implements SchemeFactory {
    public TMessageScanStandardScheme getScheme() {
      return new TMessageScanStandardScheme();
    }
  }

  private static class TMessageScanStandardScheme extends StandardScheme<TMessageScan> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TMessageScan struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // START_MESSAGE_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.startMessageID = new TMessageID();
              struct.startMessageID.read(iprot);
              struct.setStartMessageIDIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // STOP_MESSAGE_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.stopMessageID = new TMessageID();
              struct.stopMessageID.read(iprot);
              struct.setStopMessageIDIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // TOPICS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list50 = iprot.readListBegin();
                struct.topics = new ArrayList<ByteBuffer>(_list50.size);
                for (int _i51 = 0; _i51 < _list50.size; ++_i51)
                {
                  ByteBuffer _elem52; // required
                  _elem52 = iprot.readBinary();
                  struct.topics.add(_elem52);
                }
                iprot.readListEnd();
              }
              struct.setTopicsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TMessageScan struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.startMessageID != null) {
        oprot.writeFieldBegin(START_MESSAGE_ID_FIELD_DESC);
        struct.startMessageID.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.stopMessageID != null) {
        oprot.writeFieldBegin(STOP_MESSAGE_ID_FIELD_DESC);
        struct.stopMessageID.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.topics != null) {
        if (struct.isSetTopics()) {
          oprot.writeFieldBegin(TOPICS_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.topics.size()));
            for (ByteBuffer _iter53 : struct.topics)
            {
              oprot.writeBinary(_iter53);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TMessageScanTupleSchemeFactory implements SchemeFactory {
    public TMessageScanTupleScheme getScheme() {
      return new TMessageScanTupleScheme();
    }
  }

  private static class TMessageScanTupleScheme extends TupleScheme<TMessageScan> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TMessageScan struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetStartMessageID()) {
        optionals.set(0);
      }
      if (struct.isSetStopMessageID()) {
        optionals.set(1);
      }
      if (struct.isSetTopics()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetStartMessageID()) {
        struct.startMessageID.write(oprot);
      }
      if (struct.isSetStopMessageID()) {
        struct.stopMessageID.write(oprot);
      }
      if (struct.isSetTopics()) {
        {
          oprot.writeI32(struct.topics.size());
          for (ByteBuffer _iter54 : struct.topics)
          {
            oprot.writeBinary(_iter54);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TMessageScan struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.startMessageID = new TMessageID();
        struct.startMessageID.read(iprot);
        struct.setStartMessageIDIsSet(true);
      }
      if (incoming.get(1)) {
        struct.stopMessageID = new TMessageID();
        struct.stopMessageID.read(iprot);
        struct.setStopMessageIDIsSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TList _list55 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.topics = new ArrayList<ByteBuffer>(_list55.size);
          for (int _i56 = 0; _i56 < _list55.size; ++_i56)
          {
            ByteBuffer _elem57; // required
            _elem57 = iprot.readBinary();
            struct.topics.add(_elem57);
          }
        }
        struct.setTopicsIsSet(true);
      }
    }
  }

}

