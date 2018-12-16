package com.cerner.bunsen.definitions;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.profiles.ProfileProvider;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Helper functions to allow code to convert FHIR resources
 * independently of the FHIR version. Typically an implementation specific
 * to a FHIR version is provided at runtime.
 */
public abstract class FhirConversionSupport implements Serializable {

  private static final String STU3_SUPPORT_CLASS =
      "com.cerner.bunsen.definitions.stu3.Stu3FhirConversionSupport";


  public abstract String fhirType(IBase base);

  public abstract Map<String, List> compositeValues(IBase composite);

  public abstract java.util.List<IBaseResource> extractEntryFromBundle(IBaseBundle bundle,
      String resourceName);

  /**
   * Cache of FHIR contexts.
   */
  private static final Map<FhirVersionEnum, FhirConversionSupport> FHIR_SUPPORT = new HashMap();


  private static FhirConversionSupport newInstance(FhirVersionEnum fhirVersion) {

    Class fhirSupportClass;

    if (FhirVersionEnum.DSTU3.equals(fhirVersion)) {

      try {
        fhirSupportClass = Class.forName(STU3_SUPPORT_CLASS);

      } catch (ClassNotFoundException e) {

        throw new IllegalStateException(e);

      }

    } else {
      throw new IllegalArgumentException("Unsupported FHIR version: " + fhirVersion);
    }

    try {

      return (FhirConversionSupport) fhirSupportClass.newInstance();

    } catch (Exception e) {

      throw new IllegalStateException("Unable to create FHIR support class", e);
    }
  }

  /**
   * Returns the FHIR context for the given version. This is effectively a cache
   * so consuming code does not need to recreate the context repeatedly.
   *
   * @param fhirVersion the version of FHIR to use
   * @return the FhirContext
   */
  public static FhirConversionSupport supportFor(FhirVersionEnum fhirVersion) {

    synchronized (FHIR_SUPPORT) {

      FhirConversionSupport support = FHIR_SUPPORT.get(fhirVersion);

      if (support == null) {

        support = newInstance(fhirVersion);

        FHIR_SUPPORT.put(fhirVersion, support);
      }

      return support;
    }
  }

  public static FhirConversionSupport forStu3() {

    return supportFor(FhirVersionEnum.DSTU3);
  }

}
