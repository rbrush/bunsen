package com.cerner.bunsen.definitions;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.support.IContextValidationSupport;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Constructor;
import java.util.Set;

public abstract class StructureDefinitions<SDT> {

  protected static final Set<String> PRIMITIVE_TYPES =  ImmutableSet.<String>builder()
      .add("id")
      .add("boolean")
      .add("code")
      .add("markdown")
      .add("date")
      .add("instant")
      .add("datetime")
      .add("dateTime")
      .add("time")
      .add("string")
      .add("decimal")
      .add("integer")
      .add("xhtml")
      .add("unsignedInt")
      .add("positiveInt")
      .add("base64Binary")
      .add("uri")
      .build();

  private static final String STU3_DEFINITIONS_CLASS =
      "com.cerner.bunsen.definitions.stu3.Stu3StructureDefinitions";

  protected final FhirContext context;
  protected final IContextValidationSupport validationSupport;

  public StructureDefinitions(FhirContext context) {

    this.context = context;
    this.validationSupport = context.getValidationSupport();
  }



  public abstract <T> T transform(DefinitionVisitor<T> visitor,
      String resourceTypeUrl);

  public abstract FhirConversionSupport conversionSupport();

  public static StructureDefinitions create(FhirContext context) {

    Class structureDefinitionsClass;

    if (FhirVersionEnum.DSTU3.equals(context.getVersion().getVersion())) {

      try {
        structureDefinitionsClass = Class.forName(STU3_DEFINITIONS_CLASS);


      } catch (ClassNotFoundException e) {

        throw new IllegalStateException(e);

      }

      try {
        Constructor constructor = structureDefinitionsClass.getConstructor(FhirContext.class);

        return (StructureDefinitions) constructor.newInstance(context);

      } catch (Exception e) {
        throw new IllegalStateException(e);
      }

    } else {
      throw new IllegalArgumentException("Unsupported FHIR version: "
        + context.getVersion().getVersion());
    }
  }

}