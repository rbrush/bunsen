package com.cerner.bunsen.stu3;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.support.IContextValidationSupport;
import ca.uhn.fhir.parser.IParser;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import org.hl7.fhir.dstu3.hapi.validation.PrePopulatedValidationSupport;
import org.hl7.fhir.dstu3.model.StructureDefinition;


public class UsCoreStructures {

  private static void load(PrePopulatedValidationSupport support,
      IParser jsonParser,
      String resource) {

    try (InputStream input = UsCoreStructures.class
        .getClassLoader()
        .getResourceAsStream(resource)) {

      StructureDefinition definition = (StructureDefinition)
          jsonParser.parseResource(new InputStreamReader(input));

      support.addStructureDefinition(definition);

    } catch (IOException e) {

      throw new RuntimeException(e);
    }
  }

  private static void addUsCoreDefinitions(PrePopulatedValidationSupport support,
      FhirContext context) {

    IParser parser = context.newJsonParser();

    load(support, parser, "definitions/StructureDefinition-us-core-allergyintolerance.json");
    load(support, parser, "definitions/StructureDefinition-us-core-birthsex.json");
    load(support, parser, "definitions/StructureDefinition-us-core-careplan.json");
    load(support, parser, "definitions/StructureDefinition-us-core-careteam.json");
    load(support, parser, "definitions/StructureDefinition-us-core-condition.json");
    load(support, parser, "definitions/StructureDefinition-us-core-device.json");
    load(support, parser, "definitions/StructureDefinition-us-core-diagnosticreport.json");
    load(support, parser, "definitions/StructureDefinition-us-core-direct.json");
    load(support, parser, "definitions/StructureDefinition-us-core-documentreference.json");
    load(support, parser, "definitions/StructureDefinition-us-core-encounter.json");
    load(support, parser, "definitions/StructureDefinition-us-core-ethnicity.json");
    load(support, parser, "definitions/StructureDefinition-us-core-goal.json");
    load(support, parser, "definitions/StructureDefinition-us-core-immunization.json");
    load(support, parser, "definitions/StructureDefinition-us-core-location.json");
    load(support, parser, "definitions/StructureDefinition-us-core-medication.json");
    load(support, parser, "definitions/StructureDefinition-us-core-medicationrequest.json");
    load(support, parser, "definitions/StructureDefinition-us-core-medicationstatement.json");
    load(support, parser, "definitions/StructureDefinition-us-core-observationresults.json");
    load(support, parser, "definitions/StructureDefinition-us-core-organization.json");
    load(support, parser, "definitions/StructureDefinition-us-core-patient.json");
    load(support, parser, "definitions/StructureDefinition-us-core-practitioner.json");
    load(support, parser, "definitions/StructureDefinition-us-core-practitionerrole.json");
    load(support, parser, "definitions/StructureDefinition-us-core-procedure.json");
    load(support, parser, "definitions/StructureDefinition-us-core-profile-link.json");
    load(support, parser, "definitions/StructureDefinition-us-core-race.json");
    load(support, parser, "definitions/StructureDefinition-us-core-smokingstatus.json");

  }

  /**
   * Adds US Core FHIR resources to the given context.
   * @param context
   */
  public static void addUsCoreResources(FhirContext context) {

    // DefaultProfileValidationSupport defaultSupport = new DefaultProfileValidationSupport();

    IContextValidationSupport defaultSupport = context.getValidationSupport();

    PrePopulatedValidationSupport support = new PrePopulatedValidationSupport();

    List<StructureDefinition> defaultDefinitions = defaultSupport.fetchAllStructureDefinitions(context);


    for (StructureDefinition definition:  defaultDefinitions) {

      support.addStructureDefinition(definition);

    }

    addUsCoreDefinitions(support, context);


    context.setValidationSupport(support);

  }

}
