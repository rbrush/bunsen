package com.cerner.bunsen.spark.stu3;

import org.hl7.fhir.instance.model.api.IBase;

public interface RowToHapiConverter {

  public IBase toHapi(Object input);
}