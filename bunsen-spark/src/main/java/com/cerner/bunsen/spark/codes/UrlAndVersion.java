package com.cerner.bunsen.spark.codes;

/**
 * URI and version tuple used to uniquely identify a concept map, value set, or hierarchy.
 */
public class UrlAndVersion {

  private String url;

  private String version;

  public UrlAndVersion(String url, String version) {
    this.url = url;
    this.version = version;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  /**
   * Nullary constructor for use in Spark data sets.
   */
  public UrlAndVersion() {
  }

  @Override
  public int hashCode() {

    return 17 * url.hashCode() * version.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof UrlAndVersion)) {
      return false;
    }

    UrlAndVersion that = (UrlAndVersion) obj;

    return this.url.equals(that.url)
        && this.version.equals(that.version);
  }
}
