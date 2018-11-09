package com.cerner.bunsen.spark.stu3;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import org.hl7.fhir.dstu3.model.Address;
import org.hl7.fhir.dstu3.model.CodeType;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.DateType;
import org.hl7.fhir.dstu3.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.dstu3.model.Extension;
import org.hl7.fhir.dstu3.model.IntegerType;
import org.hl7.fhir.dstu3.model.Patient;
import org.hl7.fhir.dstu3.model.StringType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.junit.Test;

public class ParseResourceTest {

  private static final FhirContext context = FhirContext.forDstu3();


  private Patient patient() {

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
  public void testParse() {

    Patient patient = patient();

    IParser parser = context.newJsonParser();

    String encoded = parser.encodeResourceToString(patient);

    Patient parsedPatient = (Patient) parser.parseResource(encoded);

    System.out.println(parsedPatient.getId());
  }
}
