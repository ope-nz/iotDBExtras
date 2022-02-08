package nz.ope.iotdb.extras;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.io.IOException;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.api.exception.UDFException;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class UDTFSort implements UDTF {
  private Map<Long,Object> output = new HashMap<Long,Object>();

  int outputDataType;
  String sort = "asc";

  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
  {
    this.sort = parameters.getString("sort");

    try
    {
      configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(parameters.getDataType(0));
      outputDataType = parameters.getDataType(0).ordinal();
    }
    catch (Exception e)
    {
      System.out.println(e.toString());
    }        
  }

  public void transform(Row paramRow, PointCollector paramPointCollector) {
    if (!paramRow.isNull(0))
    {
        Long time = paramRow.getTime();
        Object value = getObject(paramRow);      

        if (value == null) return;
        this.output.put(time, value);      
    }
  }

  private Object getObject(Row paramRow) {
    switch (outputDataType) {
    case 0: // BOOLEAN
      return paramRow.getBoolean(0);
    case 1: // INT32
      return paramRow.getInt(0);
    case 2: // INT64
      return paramRow.getLong(0);
    case 3: // FLOAT
      return paramRow.getFloat(0);
    case 4: // DOUBLE
      return paramRow.getDouble(0);
    case 5: // TEXT
      return paramRow.getString(0);
    }

    return null;
  }

  public void terminate(PointCollector collector) throws IOException, UDFException, QueryProcessException
  {
    Map<Long,Object> sorted = SortMap(this.output);   

    for (Map.Entry<Long,Object> entry : sorted.entrySet())
    {      
        Long time = entry.getKey();
        Object value = entry.getValue();
        collector = putObject(collector,time,value);        
    }   
  }

  private PointCollector putObject(PointCollector collector, Long time, Object value) {
    try {
      switch (outputDataType) {
      case 0:
        collector.putBoolean(time, (Boolean) value);
        break;
      case 1:
        collector.putInt(time, (Integer) value);
        break;
      case 2:
        collector.putLong(time, (Long) value);
        break;
      case 3:
        collector.putFloat(time, (Float) value);
        break;
      case 4:
        collector.putDouble(time, (Double) value);
        break;
      case 5:
        collector.putString(time, (String) value);
        break;
      }
    } catch (Exception e)
    {
      System.out.println(e.toString());
    }

    return collector;
  }

  private Map<Long,Object> SortMap(Map<Long,Object> map)
  {
    switch (outputDataType)
    {
      case 0: // BOOLEAN
        Map<Long,Boolean> mapCopyBOOLEAN = new HashMap<>();
        for (Map.Entry<Long,Object> entry : map.entrySet()) {mapCopyBOOLEAN.put(entry.getKey(),(Boolean)entry.getValue());}
        if (sort.equalsIgnoreCase("asc")) return mapCopyBOOLEAN.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        if (sort.equalsIgnoreCase("desc")) return mapCopyBOOLEAN.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

      case 1: // INT32
        Map<Long,Integer> mapCopy = new HashMap<>();
        for (Map.Entry<Long,Object> entry : map.entrySet()) {mapCopy.put(entry.getKey(),(Integer)entry.getValue());}
        if (sort.equalsIgnoreCase("asc")) return mapCopy.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        if (sort.equalsIgnoreCase("desc")) return mapCopy.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        
      case 2: // INT64
        Map<Long,Long> mapCopyINT64 = new HashMap<>();
        for (Map.Entry<Long,Object> entry : map.entrySet()) {mapCopyINT64.put(entry.getKey(),(Long)entry.getValue());}
        if (sort.equalsIgnoreCase("asc")) return mapCopyINT64.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        if (sort.equalsIgnoreCase("desc")) return mapCopyINT64.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

      case 3: // FLOAT
        Map<Long,Float> mapCopyFLOAT = new HashMap<>();
        for (Map.Entry<Long,Object> entry : map.entrySet()) {mapCopyFLOAT.put(entry.getKey(),(Float)entry.getValue());}
        if (sort.equalsIgnoreCase("asc")) return mapCopyFLOAT.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        if (sort.equalsIgnoreCase("desc")) return mapCopyFLOAT.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

      case 4: // DOUBLE
        Map<Long,Double> mapCopyDOUBLE = new HashMap<>();
        for (Map.Entry<Long,Object> entry : map.entrySet()) {mapCopyDOUBLE.put(entry.getKey(),(Double)entry.getValue());}
        if (sort.equalsIgnoreCase("asc")) return mapCopyDOUBLE.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        if (sort.equalsIgnoreCase("desc")) return mapCopyDOUBLE.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));        

      case 5: // TEXT
        Map<Long,String> mapCopyTEXT = new HashMap<>();
        for (Map.Entry<Long,Object> entry : map.entrySet()) {mapCopyTEXT.put(entry.getKey(),(String)entry.getValue());}
        if (sort.equalsIgnoreCase("asc")) return mapCopyTEXT.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        if (sort.equalsIgnoreCase("desc")) return mapCopyTEXT.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

      }
    
    return map;
  }
}
