package com.cerner.bunsen.spark.stu3;


import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.hl7.fhir.dstu3.model.Base;

public abstract class HapiToSparkConverter {

  public interface HapiFieldSetter {

    public void setField(Base parentObject,
        BaseRuntimeChildDefinition fieldToSet,
        Object sparkObject);
  }

  public abstract Object toSpark(Object input);

  public abstract DataType getDataType();

  public String extensionUrl() {
    return null;
  }
  public String getElementType() {
    return null;
  }

  public abstract HapiFieldSetter toHapiConverter(
      BaseRuntimeElementDefinition... elementDefinitions);
}
