package com.cerner.bunsen.spark;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import com.cerner.bunsen.definitions.StructureDefinitions;
import java.util.List;
import java.util.Collections;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
      StructureDefinitions<HapiToSparkConverter> structureDefinitions,
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

    StructureDefinitions structureDefinitions = StructureDefinitions.create(context);

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

  public String getResourceType() {
    return hapiToSparkConverter.getElementType();
  }

  public Dataset<Row> emptyDataFrame(SparkSession spark) {

    return toDataFrame(spark, Collections.emptyList());
  }

  public Dataset<Row> toDataFrame(SparkSession spark, List<IBaseResource> resources) {

    List<Row> rows = resources.stream()
        .map(resource -> resourceToRow(resource))
        .collect(Collectors.toList());

    return spark.createDataFrame(rows, getSchema());
  }

}
