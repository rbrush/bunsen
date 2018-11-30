package com.cerner.bunsen.spark.stu3;

import ca.uhn.fhir.context.FhirContext;
import com.cerner.bunsen.stu3.UsCoreStructures;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collections;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.hl7.fhir.dstu3.model.Address;
import org.hl7.fhir.dstu3.model.CodeType;
import org.hl7.fhir.dstu3.model.CodeableConcept;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.Condition;
import org.hl7.fhir.dstu3.model.DateTimeType;
import org.hl7.fhir.dstu3.model.DateType;
import org.hl7.fhir.dstu3.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.dstu3.model.Extension;
import org.hl7.fhir.dstu3.model.Identifier;
import org.hl7.fhir.dstu3.model.IntegerType;
import org.hl7.fhir.dstu3.model.Narrative;
import org.hl7.fhir.dstu3.model.Observation;
import org.hl7.fhir.dstu3.model.Observation.ObservationComponentComponent;
import org.hl7.fhir.dstu3.model.Patient;
import org.hl7.fhir.dstu3.model.Quantity;
import org.hl7.fhir.dstu3.model.Reference;
import org.hl7.fhir.dstu3.model.StringType;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.utilities.xhtml.NodeType;
import org.hl7.fhir.utilities.xhtml.XhtmlNode;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SparkRowConverterTest {

  public static final String US_CORE_BIRTHSEX = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex";
  public static final String US_CORE_ETHNICITY = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity";
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

  private static final String US_CORE_PATIENT =
      "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient";

  private static final String US_CORE_OBSERVATION =
      "http://hl7.org/fhir/us/core/StructureDefinition/us-core-observationresults";

  private static final String US_CORE_CONDITION =
      "http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition";


  private static final Patient testPatient = newPatient();

  private static Dataset<Row> testPatientDataset;

  private static Patient testPatientDecoded;

  private static final Observation testObservation = newObservation();

  private static Dataset<Row> testObservationDataset;

  private static Observation testObservationDecoded;

  private static final Condition testCondition = newCondition();

  private static Dataset<Row> testConditionDataset;

  private static Condition testConditionDecoded;

  @BeforeClass
  public static void loadDefinition() throws IOException {

    fhirContext = FhirContext.forDstu3();

    UsCoreStructures.addUsCoreResources(fhirContext);

    SparkRowConverter patientConverter = SparkRowConverter.forResource(fhirContext,
        US_CORE_PATIENT);

    Row testPatientRow = patientConverter.resourceToRow(testPatient);

    testPatientDataset = spark.createDataFrame(Collections.singletonList(testPatientRow),
        patientConverter.getSchema());

    testPatientDecoded = (Patient) patientConverter.rowToResource(testPatientDataset.head());

    SparkRowConverter observationConverter = SparkRowConverter.forResource(fhirContext,
        US_CORE_OBSERVATION);

    Row testObservationRow = observationConverter.resourceToRow(testObservation);

    testObservationDataset = spark.createDataFrame(Collections.singletonList(testObservationRow),
        observationConverter.getSchema());

    testObservationDecoded =
        (Observation) observationConverter.rowToResource(testObservationDataset.head());

    SparkRowConverter conditionConverter = SparkRowConverter.forResource(fhirContext,
        US_CORE_CONDITION);

    Row testConditionRow = conditionConverter.resourceToRow(testCondition);

    testConditionDataset = spark.createDataFrame(Collections.singletonList(testConditionRow),
        conditionConverter.getSchema());

    testConditionDecoded =
        (Condition) conditionConverter.rowToResource(testConditionDataset.head());
  }

  /**
   * Returns a FHIR Condition for testing purposes.
   */
  public static Condition newCondition() {

    Condition condition = new Condition();

    // Condition based on example from FHIR:
    // https://www.hl7.org/fhir/condition-example.json.html
    condition.setId("Condition/example");

    condition.setLanguage("en_US");

    // Narrative text
    Narrative narrative = new Narrative();
    narrative.setStatusAsString("generated");
    narrative.setDivAsString("This data was generated for test purposes.");
    XhtmlNode node = new XhtmlNode();
    node.setNodeType(NodeType.Text);
    node.setValue("Severe burn of left ear (Date: 24-May 2012)");
    condition.setText(narrative);

    condition.setSubject(new Reference("Patient/12345").setDisplay("Here is a display for you."));

    condition.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

    // Condition code
    CodeableConcept code = new CodeableConcept();
    code.addCoding()
        .setSystem("http://snomed.info/sct")
        .setCode("39065001")
        .setDisplay("Severe");
    condition.setSeverity(code);

    // Severity code
    CodeableConcept severity = new CodeableConcept();
    severity.addCoding()
        .setSystem("http://snomed.info/sct")
        .setCode("24484000")
        .setDisplay("Burn of ear")
        .setUserSelected(true);
    condition.setSeverity(severity);

    // Onset date time
    DateTimeType onset = new DateTimeType();
    onset.setValueAsString("2012-05-24");
    condition.setOnset(onset);

    return condition;
  }

  private static Observation newObservation() {
    Observation observation = new Observation();

    observation.setId("blood-pressure");

    Identifier identifier = observation.addIdentifier();
    identifier.setSystem("urn:ietf:rfc:3986");
    identifier.setValue("urn:uuid:187e0c12-8dd2-67e2-99b2-bf273c878281");

    observation.setStatus(Observation.ObservationStatus.FINAL);


    CodeableConcept obsCode = new CodeableConcept();


    observation.setCode(obsCode);

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

  private static Patient newPatient() {

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

    birthSex.setUrl(US_CORE_BIRTHSEX);
    birthSex.setValue(new CodeType("M"));

    Extension ethnicity = patient.addExtension();
    ethnicity.setUrl(US_CORE_ETHNICITY);
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
  public void testInteger() {

    Assert.assertEquals(((IntegerType) testPatient.getMultipleBirth()).getValue(),
        testPatientDataset.select("multipleBirth.integer").head().get(0));

    Assert.assertEquals(((IntegerType) testPatient.getMultipleBirth()).getValue(),
        ((IntegerType) testPatientDecoded.getMultipleBirth()).getValue());
  }

  @Test
  public void testDecimal() {

    BigDecimal originalDecimal = ((Quantity) testObservation.getValue()).getValue();

    // Use compareTo since equals checks scale as well.
    Assert.assertTrue(originalDecimal.compareTo(
        (BigDecimal) testObservationDataset.select(
            "value.quantity.value").head().get(0)) == 0);

    Assert.assertEquals(originalDecimal.compareTo(
        ((Quantity) testObservationDecoded
            .getValue())
            .getValue()), 0);
  }

  @Test
  public void testChoice() throws FHIRException {

    // Ensure that a decoded choice type matches the original
    Assert.assertTrue(testPatient.getMultipleBirth()
        .equalsDeep(testPatientDecoded.getMultipleBirth()));

  }

  @Test
  public void testBoundCode() {

    Assert.assertEquals(testObservation.getStatus().toCode(),
        testObservationDataset.select("status").head().get(0));

    Assert.assertEquals(testObservation.getStatus(),
        testObservationDecoded.getStatus());
  }

  @Test
  public void testCoding() {

    Coding testCoding = testCondition.getSeverity().getCodingFirstRep();
    Coding decodedCoding = testConditionDecoded.getSeverity().getCodingFirstRep();

    // Codings are a nested array, so we explode them into a table of the coding
    // fields so we can easily select and compare individual fields.
    Dataset<Row> severityCodings = testConditionDataset
        .select(functions.explode(testConditionDataset.col("severity.coding"))
            .alias("coding"))
        .select("coding.*") // Pull all fields in the coding to the top level.
        .cache();

    Assert.assertEquals(testCoding.getCode(),
        severityCodings.select("code").head().get(0));
    Assert.assertEquals(testCoding.getCode(),
        decodedCoding.getCode());

    Assert.assertEquals(testCoding.getSystem(),
        severityCodings.select("system").head().get(0));
    Assert.assertEquals(testCoding.getSystem(),
        decodedCoding.getSystem());

    Assert.assertEquals(testCoding.getUserSelected(),
        severityCodings.select("userSelected").head().get(0));
    Assert.assertEquals(testCoding.getUserSelected(),
        decodedCoding.getUserSelected());

    Assert.assertEquals(testCoding.getDisplay(),
        severityCodings.select("display").head().get(0));
    Assert.assertEquals(testCoding.getDisplay(),
        decodedCoding.getDisplay());
  }

  @Test
  public void testSingleReference() {

    Assert.assertEquals(testCondition.getSubject().getReference(),
        testConditionDataset.select("subject.reference").head().get(0));

    Assert.assertEquals("12345", testConditionDataset
        .select("subject.patientId").head().get(0));

    Assert.assertEquals(testCondition.getSubject().getReference(),
        testConditionDecoded.getSubject().getReference());
  }

  @Test
  public void testMultiReferenceTypes() {

    // Row containing the general practitioner from our dataset.
    Row practitioner = testPatientDataset
        .select(functions.explode(functions.col("generalpractitioner")))
        .select("col.organizationId", "col.practitionerId")
        .head();

    String organizationId = practitioner.getString(0);
    String practitionerId = practitioner.getString(1);

    // The reference is not of this type, so the field should be null.
    Assert.assertNull(organizationId);

    // The field with the expected prefix should match the original data.
    Assert.assertEquals(testPatient.getGeneralPractitionerFirstRep().getReference(),
        "Practitioner/" + practitionerId);

    Assert.assertEquals(testCondition.getSubject().getReference(),
        testConditionDecoded.getSubject().getReference());
  }

  @Test
  public void testSimpleExtension() {

    String testBirthSex = testPatient
        .getExtensionsByUrl(US_CORE_BIRTHSEX)
        .get(0)
        .getValueAsPrimitive()
        .getValueAsString();

    String decodedBirthSex = testPatientDecoded
        .getExtensionsByUrl(US_CORE_BIRTHSEX)
        .get(0)
        .getValueAsPrimitive()
        .getValueAsString();

    Assert.assertEquals(testBirthSex, decodedBirthSex);

    Assert.assertEquals(testBirthSex,
        testPatientDataset.select("birthSex").head().get(0));
  }

  @Test
  public void testNestedExtension() {

    Extension testEthnicity = testPatient
        .getExtensionsByUrl(US_CORE_ETHNICITY)
        .get(0);

    Coding testOmbCategory = (Coding) testEthnicity
        .getExtensionsByUrl("ombCategory")
        .get(0)
        .getValue();

    String testText = testEthnicity
        .getExtensionsByUrl("text")
        .get(0)
        .getValueAsPrimitive()
        .getValueAsString();

    Extension decodedEthnicity = testPatientDecoded
        .getExtensionsByUrl(US_CORE_ETHNICITY)
        .get(0);

    Coding decodedOmbCategory = (Coding) decodedEthnicity
        .getExtensionsByUrl("ombCategory")
        .get(0)
        .getValue();

    String decodedText = decodedEthnicity
        .getExtensionsByUrl("text")
        .get(0)
        .getValueAsPrimitive()
        .getValueAsString();

    Assert.assertTrue(testOmbCategory.equalsDeep(decodedOmbCategory));
    Assert.assertEquals(testText, decodedText);

    Row ombCategoryRow = testPatientDataset.select(
        "ethnicity.ombcategory.system",
        "ethnicity.ombcategory.code",
        "ethnicity.ombcategory.display")
        .head();

    Assert.assertEquals(testOmbCategory.getSystem(), ombCategoryRow.get(0));
    Assert.assertEquals(testOmbCategory.getCode(), ombCategoryRow.get(1));
    Assert.assertEquals(testOmbCategory.getDisplay(), ombCategoryRow.get(2));

    Row textRow = testPatientDataset.select("ethnicity.text").head();

    Assert.assertEquals(testText, textRow.get(0));
  }
}
