
package de.dfki.sds.rdf2rdb;

/**
 * 
 */
public class MavenExportConfig {
    
    private String artifactId;
    private String groupId;
    private String version;
    private String name;
    private String desc;
    private String classpath;
    private String jarParams;
    private String pomExtra;

    public String getArtifactId() {
        return artifactId;
    }

    public void setArtifactId(String artifactId) {
        this.artifactId = artifactId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getClasspath() {
        return classpath;
    }

    public void setClasspath(String classpath) {
        this.classpath = classpath;
    }

    public String getJarParams() {
        return jarParams;
    }

    public void setJarParams(String jarParams) {
        this.jarParams = jarParams;
    }

    public String getPomExtra() {
        return pomExtra;
    }

    public void setPomExtra(String pomExtra) {
        this.pomExtra = pomExtra;
    }
    
    
}
