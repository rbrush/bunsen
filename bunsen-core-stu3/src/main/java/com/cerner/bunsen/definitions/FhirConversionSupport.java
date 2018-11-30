package com.cerner.bunsen.definitions;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.hl7.fhir.instance.model.api.IBase;

/**
 * Helper functions to allow code to convert FHIR resources
 * independently of the FHIR version. Typically an implementation specific
 * to a FHIR version is provided at runtime.
 */
public interface FhirConversionSupport extends Serializable {

  public String fhirType(IBase base);

  public Map<String, List> compositeValues(IBase composite);

}
