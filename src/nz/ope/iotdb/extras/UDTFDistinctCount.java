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

public class UDTFDistinctCount implements UDTF {
  private Map<Object, Long> output = new HashMap<Object, Long>();

  int outputDataType;
  String sort = null;

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
      Object key = getObject(paramRow);      

      if (key == null) return;

      if (!this.output.containsKey(key))
      {
        this.output.put(key, 1L);
      }
      else
      {
        Long count = 1 + this.output.get(key);
        this.output.put(key, count);
      }
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
    Map<Object, Long> sorted = SortMap(this.output);   

    for (Map.Entry<Object, Long> entry : sorted.entrySet())
    {      
        Object key = entry.getKey();
        Long count = entry.getValue();
        collector = putObject(collector,key,count);        
      }   
  }

  private PointCollector putObject(PointCollector collector, Object key, Long count) {
    try {
      switch (outputDataType) {
      case 0:
        collector.putBoolean(count, (Boolean) key);
        break;
      case 1:
        collector.putInt(count, (int) key);
        break;
      case 2:
        collector.putLong(count, (long) key);
        break;
      case 3:
        collector.putFloat(count, (float) key);
        break;
      case 4:
        collector.putDouble(count, (double) key);
        break;
      case 5:
        collector.putString(count, (String) key);
        break;
      }
    } catch (Exception e)
    {
      System.out.println(e.toString());
    }

    return collector;
  }

  private Map<Object, Long> SortMap(Map<Object, Long> map)
  { 
    if (sort.equalsIgnoreCase("asc"))
    {
      return map.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }

    if (sort.equalsIgnoreCase("desc"))
    {
      return map.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }
    
    return map;
  }
}
