package rdf2rdb;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import static java.util.stream.Collectors.toList;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;

/**
 *

 */
public class LatexTable {
    
    private List<Column> columns;
    private List<Object[]> rows;
    
    public LatexTable() {
        columns = new ArrayList<>();
        rows = new ArrayList<>();
    }
    
    public void addColumn(String abbrev, String alignment) {
        columns.add(new Column(abbrev, alignment));
    }
    
    public void addColumn(String abbrev, String description, String alignment, boolean showInLatexTable) {
        columns.add(new Column(abbrev, description, alignment, showInLatexTable));
    }
    
    public void addRow(Object... data) {
        rows.add(data);
    }
    
    private class Column {
        String abbrev;
        String description;
        String alginment;
        boolean showInLatexTable;

        public Column(String abbrev, String alginment) {
            this.abbrev = abbrev;
            this.alginment = alginment;
        }

        public Column(String abbrev, String description, String alginment, boolean showInLatexTable) {
            this.abbrev = abbrev;
            this.description = description;
            this.alginment = alginment;
            this.showInLatexTable = showInLatexTable;
        }
        
    }
    
    public String getCaption() {
        StringBuilder sb = new StringBuilder();
        sb.append("\\caption{\n");
        
        List<Column> filtered = columns.stream().filter(c -> c.showInLatexTable).collect(toList());
        
        for(int i = 0; i < filtered.size(); i++) {
            Column c = filtered.get(i);
            if(c.showInLatexTable) {
                //sb.append(c.abbrev).append(" -- ").append(c.description);
                sb.append(c.description).append(" (").append(c.abbrev).append(")");
                
                if(i != filtered.size() - 1)
                    sb.append(",\n");
            }
        }
        sb.append("\n}");
        return sb.toString();
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        //List<Column> filtered = colums.stream().filter(c -> c.showInLatexTable).collect(toList());
        
        StringJoiner alignHeader = new StringJoiner("|", "|", "|");
        for(Column col : columns) {
            if(!col.showInLatexTable)
                continue;
            
            alignHeader.add(col.alginment);
        }

        sb.append("\\begin{tabular}{"+alignHeader.toString()+"}\n");
        sb.append("\\hline\n");
        StringJoiner header1 = new StringJoiner(" & ");
        for(Column col : columns) {
            if(!col.showInLatexTable)
                continue;
            
            header1.add(col.abbrev);
        }
        sb.append(header1.toString() + " \\\\\n");
        sb.append("\\hline\n");
        sb.append("\\hline\n");
        
        for(Object[] row : rows) {
            StringJoiner sj = new StringJoiner(" & ");
            
            for(int i = 0; i < row.length; i++) {
                if(!columns.get(i).showInLatexTable)
                    continue;
                
                sj.add(row[i].toString());
            }
            
            sb.append(sj.toString() + " \\\\");
            sb.append("\\hline\n");
        }
        
        sb.append("\\end{tabular}\n");
        
        return sb.toString();
    }
    
    public String toCSV() throws IOException {
        StringWriter sw = new StringWriter();
        CSVPrinter p = CSVFormat.DEFAULT.print(sw);
        
        //head
        for(Column c : columns) {
            p.print(c.description + "(" + c.abbrev + ")");
        }
        p.println();
        
        //body
        for(Object[] row : rows) {
            p.printRecord(row);
        }
        
        p.close();
        return sw.toString();
    }
    
    public void saveCSV(File file) {
        try {
            FileUtils.writeStringToFile(file, toCSV(), StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
    
}
