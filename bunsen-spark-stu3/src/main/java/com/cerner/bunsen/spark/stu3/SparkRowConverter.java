package com.cerner.bunsen.spark.stu3;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import com.cerner.bunsen.definitions.stu3.Stu3StructureDefinitions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;


/**
 * Support for converting FHIR resources to Apache Spark rows and vice versa.
 */
public class SparkRowConverter {

  private final HapiToSparkConverter hapiToSparkConverter;

  private final RowToHapiConverter sparkToHapiConverter;

  private final DefinitionToSparkVisitor visitor;

  SparkRowConverter(FhirContext context,
      Stu3StructureDefinitions structureDefinitions,
      String resourceTypeUrl) {

    this.visitor = new DefinitionToSparkVisitor(structureDefinitions.conversionSupport());

    this.hapiToSparkConverter = structureDefinitions.transform(visitor, resourceTypeUrl);

    RuntimeResourceDefinition resourceDefinition =
        context.getResourceDefinition(hapiToSparkConverter.getElementType());

    this.sparkToHapiConverter  =
        (RowToHapiConverter) hapiToSparkConverter.toHapiConverter(resourceDefinition);
  }

  public static SparkRowConverter forResource(FhirContext context,
      String resourceTypeUrl) {

    Stu3StructureDefinitions structureDefinitions = new Stu3StructureDefinitions(context);

    return new SparkRowConverter(context, structureDefinitions, resourceTypeUrl);
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
