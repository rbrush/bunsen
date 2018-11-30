package com.cerner.bunsen.spark.stu3;

import ca.uhn.fhir.context.FhirContext;
import com.cerner.bunsen.stu3.UsCoreStructures;
import java.io.IOException;
import java.util.Collections;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.hl7.fhir.dstu3.model.Address;
import org.hl7.fhir.dstu3.model.CodeType;
import org.hl7.fhir.dstu3.model.CodeableConcept;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.DateTimeType;
import org.hl7.fhir.dstu3.model.DateType;
import org.hl7.fhir.dstu3.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.dstu3.model.Extension;
import org.hl7.fhir.dstu3.model.Identifier;
import org.hl7.fhir.dstu3.model.IntegerType;
import org.hl7.fhir.dstu3.model.Observation;
import org.hl7.fhir.dstu3.model.Observation.ObservationComponentComponent;
import org.hl7.fhir.dstu3.model.Patient;
import org.hl7.fhir.dstu3.model.Quantity;
import org.hl7.fhir.dstu3.model.StringType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class HapiConversionTest {

  private static SparkSession spark;

  /**
   * Set up Spark.
   */
  @BeforeClass
  public static void setUp() {
    spark = SparkSession.builder()
        .master("local[*]")
        .appName("testing")
        .getOrCreate();
  }

  @AfterClass
  public static void tearDown() {
    spark.stop();
  }


  static FhirContext fhirContext;

  static SparkRowConverter patientConverter;

  static SparkRowConverter resultConverter;


  @BeforeClass
  public static void loadDefinition() throws IOException {

    fhirContext = FhirContext.forDstu3();

    UsCoreStructures.addUsCoreResources(fhirContext);

    patientConverter = SparkRowConverter.forResource(fhirContext,
        "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient");

    resultConverter = SparkRowConverter.forResource(fhirContext,
        "http://hl7.org/fhir/us/core/StructureDefinition/us-core-observationresults");
  }

  @Test
  public void testResultStruct() {

    patientConverter.getSchema().printTreeString();

    Observation observation = newObservation();

    observation.setEffective(new DateTimeType("2017-02-05T02:00:00.000+00:00"));

    Row row = resultConverter.resourceToRow(observation);

    System.out.println(row);

    Dataset<Row> df = spark.createDataFrame(Collections.singletonList(row),
        resultConverter.getSchema());

    df.select(functions.col("value.quantity.*"),
        functions.explode(functions.col("component")).alias("comps"))
        .select(functions.col("*"),
            functions.explode(functions.col("comps.code.coding")).alias("coding"))
        .select("*", "coding.*")
        .show();
  }

  private Observation newObservation() {
    Observation observation = new Observation();

    observation.setId("blood-pressure");

    Identifier identifier = observation.addIdentifier();
    identifier.setSystem("urn:ietf:rfc:3986");
    identifier.setValue("urn:uuid:187e0c12-8dd2-67e2-99b2-bf273c878281");

    observation.setStatus(Observation.ObservationStatus.FINAL);

    Quantity quantity = new Quantity();
    quantity.setValue(new java.math.BigDecimal("123.45"));
    quantity.setUnit("mm[Hg]");
    quantity.setSystem("http://unitsofmeasure.org");
    observation.setValue(quantity);

    ObservationComponentComponent component = observation.addComponent();

    CodeableConcept code = new CodeableConcept()
        .addCoding(new Coding()
            .setCode("abc")
            .setSystem("PLACEHOLDER"));

    component.setCode(code);

    return observation;
  }

  private Patient newPatient() {

    Patient patient = new Patient();

    patient.setId("test-patient");
    patient.setGender(AdministrativeGender.MALE);
    patient.setActive(true);
    patient.setMultipleBirth(new IntegerType(1));

    patient.setBirthDateElement(new DateType("1945-01-02"));

    Address address = patient.addAddress();

    patient.addGeneralPractitioner().setReference("Practitioner/12345");

    address.addLine("123 Fake Street");
    address.setCity("Chicago");
    address.setState("IL");
    address.setDistrict("12345");

    Extension birthSex = patient.addExtension();

    birthSex.setUrl("http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex");
    birthSex.setValue(new CodeType("M"));

    Extension ethnicity = patient.addExtension();
    ethnicity.setUrl("http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity");
    ethnicity.setValue(null);

    // Add category to ethnicity extension
    Extension ombCategory = ethnicity.addExtension();

    Coding ombCoding = new Coding();

    ombCoding.setSystem("urn:oid:2.16.840.1.113883.6.238");
    ombCoding.setCode("2186-5");
    ombCoding.setDisplay("Not Hispanic or Latino");

    ombCategory.setUrl("ombCategory");
    ombCategory.setValue(ombCoding);

    // Add text display to ethnicity extension
    Extension ethnicityText = ethnicity.addExtension();
    ethnicityText.setUrl("text");
    ethnicityText.setValue(new StringType("Not Hispanic or Latino"));

    return patient;
  }

  @Test
  public void parsePatient() {


    // ((StructType) converter.getDataType()).printTreeString();

    Patient patient = newPatient();

    Row row = patientConverter.resourceToRow(patient);

    Patient decoded = (Patient) patientConverter.rowToResource(row);

    String decodedString = fhirContext.newJsonParser().encodeResourceToString(decoded);

    System.out.println(decodedString);
  }


  @Test
  public void parseObservation() {

    Observation observation = newObservation();

    Row row = resultConverter.resourceToRow(observation);

    Observation decoded = (Observation) resultConverter.rowToResource(row);

    String decodedString = fhirContext.newJsonParser().encodeResourceToString(decoded);

    System.out.println(decodedString);
  }

  @Test
  public void testCreatesStruct() {

    Patient patient = newPatient();

    String patientString = fhirContext.newJsonParser().encodeResourceToString(patient);

    System.out.println(patientString);

    Row row = patientConverter.resourceToRow(patient);

    System.out.println(row);

    Dataset<Row> df = spark.createDataFrame(Collections.singletonList(row),
        patientConverter.getSchema());

    df.selectExpr("ethnicity.ombCategory.*",
        "ethnicity.text",
        "generalPractitioner[0].reference",
        "generalPractitioner[0].practitionerId")
        .where("birthsex = 'M'")
        .show();
  }

}
