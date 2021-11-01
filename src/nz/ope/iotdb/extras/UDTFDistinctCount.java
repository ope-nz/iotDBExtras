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
  private Map<String, Long> output = new HashMap<String, Long>();

  public void beforeStart(UDFParameters paramUDFParameters, UDTFConfigurations paramUDTFConfigurations) {
    paramUDTFConfigurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(TSDataType.TEXT);
  }

  public void transform(Row paramRow, PointCollector paramPointCollector) {
    if (!paramRow.isNull(0)) {      
      String key = paramRow.getString(0);
      if (!this.output.containsKey(key))
      {
        this.output.put(key, 1L);
      }
      else {
        Long count = 1 + this.output.get(key);
        this.output.put(key, count);
      }
    }
  }

  public void terminate(PointCollector collector) throws IOException,UDFException,QueryProcessException {
    for (Map.Entry<String, Long> entry : this.output.entrySet()) {
      String key = entry.getKey();
      Long count = entry.getValue();
      collector.putString(count, key);
    }
  }
}
