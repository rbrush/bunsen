package com.cerner.bunsen.definitions.stu3;

import com.cerner.bunsen.definitions.FhirConversionSupport;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hl7.fhir.dstu3.model.Base;
import org.hl7.fhir.dstu3.model.Property;
import org.hl7.fhir.instance.model.api.IBase;

public class Stu3FhirConversionSupport implements FhirConversionSupport {

  public String fhirType(IBase base) {

    return ((Base) base).fhirType();
  }

  public Map<String, List> compositeValues(IBase composite) {

    List<Property> children = ((Base) composite).children();

    if (children == null) {

      return null;
    } else {

      return children.stream()
          .filter(property -> property.hasValues())
          .collect(Collectors.toMap(Property::getName,
              property -> property.getValues()));
    }
  }
}
