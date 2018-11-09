package com.cerner.bunsen.spark.stu3;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import com.cerner.bunsen.definitions.StructureDefinitions;
import com.cerner.bunsen.spark.stu3.HapiToSparkConverter.HapiFieldSetter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.dstu3.model.StructureDefinition;
import org.hl7.fhir.instance.model.api.IBaseResource;


/**
 * Support for converting FHIR resources to Apache Spark rows and vice versa.
 */
public class SparkRowConverter {

  private final HapiToSparkConverter hapiToSparkConverter;

  private final RowToHapiConverter sparkToHapiConverter;

  SparkRowConverter(FhirContext context,
      StructureDefinitions structureDefinitions,
      StructureDefinition definition) {

    this.hapiToSparkConverter =
        (HapiToSparkConverter) structureDefinitions.transform(definition);

    RuntimeResourceDefinition resourceDefinition =
        context.getResourceDefinition(hapiToSparkConverter.getElementType());

    this.sparkToHapiConverter  =
        (RowToHapiConverter) hapiToSparkConverter.toHapiConverter(resourceDefinition);
  }

  public static SparkRowConverter forResource(FhirContext context,
      String resourceTypeUrl) {

    StructureDefinition definition = (StructureDefinition) context.getValidationSupport()
        .fetchStructureDefinition(context, resourceTypeUrl);

    return forResource(context, definition);
  }

  public static SparkRowConverter forResource(FhirContext context,
      StructureDefinition definition) {

    DefinitionToSparkVisitor visitor = new DefinitionToSparkVisitor();
    StructureDefinitions structureDefinitions = new StructureDefinitions(visitor, context);

    return new SparkRowConverter(context, structureDefinitions, definition);
  }

  public Row resourceToRow(IBaseResource resource) {

    return (Row) hapiToSparkConverter.toSpark(resource);
  }

  public IBaseResource rowToResource(Row row) {

    return (IBaseResource) sparkToHapiConverter.toHapi(row);
  }

  public StructType getSchema() {

    return (StructType) hapiToSparkConverter.getDataType();
  }
}
