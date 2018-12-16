package com.cerner.bunsen.spark;

import org.hl7.fhir.instance.model.api.IBase;

interface RowToHapiConverter {

  public IBase toHapi(Object input);
}