package nz.ope.iotdb.extras;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
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

  /*
  switch (type) {
    case 0:
      return ;
    case 1:
      return ;
    case 2:
      return ;
    case 3:
      return ;
    case 4:
      return ;
    case 5:
      return ;
  } 
  */

  public void beforeStart(UDFParameters paramUDFParameters, UDTFConfigurations paramUDTFConfigurations) {
        try{
          paramUDTFConfigurations.setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(paramUDFParameters.getDataType(0));

          outputDataType = paramUDFParameters.getDataType(0).ordinal();
      } catch (Exception e) {
        System.out.println(e.toString());
      }

      System.out.println(outputDataType);
        
  }

  public void transform(Row paramRow, PointCollector paramPointCollector) {
    if (!paramRow.isNull(0))
    {
      Object key = null;

      switch (outputDataType) //paramRow.getDataType(0).ordinal())
      {
        case 0: //BOOLEAN
          key = paramRow.getBoolean(0);
          break;
        case 1: //INT32
          key = paramRow.getInt(0);
          break;
        case 2: //INT64
          key = paramRow.getLong(0);
          break;
        case 3: //FLOAT
          key = paramRow.getFloat(0);
          break;
        case 4: //DOUBLE
          key = paramRow.getDouble(0);
          break;
        case 5: //TEXT
          key = paramRow.getString(0);
          break;        
      }

      if (key == null) return;

      if (!this.output.containsKey(key)) {
        this.output.put(key, 1L);
      } else {
        Long count = 1 + this.output.get(key);
        this.output.put(key, count);
      }
    }
  }

  public void terminate(PointCollector collector) throws IOException, UDFException, QueryProcessException {
    for (Map.Entry<Object, Long> entry : this.output.entrySet()) {
      Object key = entry.getKey();
      Long count = entry.getValue();

      switch (outputDataType)
      {
        case 0:
          collector.putBoolean(count, (Boolean)key);
          break;
        case 1:
          collector.putInt(count, (int)key);
          break;
        case 2:
          collector.putLong(count, (long)key);
          break;
        case 3:
          collector.putFloat(count, (float)key);
          break;
        case 4:
          collector.putDouble(count, (double)key);
          break;
        case 5:
          collector.putString(count, (String) key);
          break;        
      }
    }
  }
}
