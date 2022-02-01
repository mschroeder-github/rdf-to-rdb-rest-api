package de.dfki.sds.rdf2rdb;

import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;

/**
 *
 */
public class Warning {

    private String text;
    private Statement relatedStatement;
    private Resource relatedResource;

    public Warning(String text) {
        this.text = text;
    }

    public Warning(String text, Statement relatedStatement) {
        this.text = text;
        this.relatedStatement = relatedStatement;
    }

    public String getText() {
        return text;
    }

    public Statement getRelatedStatement() {
        return relatedStatement;
    }

    public void setRelatedStatement(Statement relatedStatement) {
        this.relatedStatement = relatedStatement;
    }

    public Resource getRelatedResource() {
        return relatedResource;
    }

    public void setRelatedResource(Resource relatedResource) {
        this.relatedResource = relatedResource;
    }

    @Override
    public String toString() {
        return "Warning{" + "text=" + text + ", relatedStatement=" + relatedStatement + '}';
    }

}
