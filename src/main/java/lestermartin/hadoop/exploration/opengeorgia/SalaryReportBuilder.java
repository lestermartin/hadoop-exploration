package lestermartin.hadoop.exploration.opengeorgia;


/**
 * class is NOT thread safe!!
 */
public class SalaryReportBuilder {

    private String workingText;

    /**
     * ON PURPOSE I'm NOT using something more sophisticated such as Apache's Commons...
     */
    public SalaryReport buildFromInputCsvFormat(String textAsCsv) {
        workingText = textAsCsv;
        SalaryReport salaryReport = new SalaryReport();

        salaryReport.setName( parseNextValueAsString() );
        salaryReport.setTitle( parseNextValueAsString() );

        salaryReport.setSalary( parseNextValueAsFloat() );
        salaryReport.setTravel( parseNextValueAsFloat() );

        salaryReport.setOrgType( parseNextValueAsString() );
        salaryReport.setOrg( parseNextValueAsString() );

        salaryReport.setYear( Integer.valueOf( workingText.substring(0) ).intValue() );

        //sanity check
        if( salaryReport.getYear() > 2050  ||  salaryReport.getYear() < 2000 ) {
            throw new IllegalStateException("unable to correct parse string '" + textAsCsv +
                    "' -- incorrectly parsed into '" + salaryReport.toString() + "'");
        }

        return salaryReport;
    }


    private String parseNextValueAsString() {
        String nextValue = null;
        //field might, or might not, start with double-quotes
        if(workingText.startsWith("\"")) {
            int endOfValue = workingText.indexOf("\"", 1);
            nextValue = workingText.substring(1, endOfValue);
            workingText = workingText.substring(endOfValue + 2);
        } else {
            int endOfValue = workingText.indexOf(",", 0);
            nextValue = workingText.substring(0, endOfValue);
            workingText = workingText.substring(endOfValue + 1);
        }
        return nextValue;
    }

    private float parseNextValueAsFloat() {
        String floatAsString = parseNextValueAsString();
        floatAsString = floatAsString.replace("$", "");
        floatAsString = floatAsString.replace(",", "");
        floatAsString = floatAsString.replace(" ", "");

        return Float.valueOf(floatAsString).floatValue();
    }

}
