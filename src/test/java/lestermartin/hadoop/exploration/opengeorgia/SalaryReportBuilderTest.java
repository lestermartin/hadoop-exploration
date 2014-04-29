package lestermartin.hadoop.exploration.opengeorgia;

import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;


/**
 * rough set of test cases due to crazy parser/builder process
 */
public class SalaryReportBuilderTest {

    private SalaryReportBuilder builder;

    @Before
    public void setupBuilder() {
        builder = new SalaryReportBuilder();
    }


    @Test
    public void testBuildNameWithQuotes() {
        SalaryReport salaryReport = builder.buildFromInputCsvFormat(
                "\"ADAMS,CHRISTOPHER HENRY\",GRADE 6 TEACHER,\"50,889.63\",\"1,817.35\",LBOE,ATLANTA INDEPENDENT SCHOOL SYSTEM,2010");

        assertEquals("name must be 'ADAMS,CHRISTOPHER HENRY'", "ADAMS,CHRISTOPHER HENRY", salaryReport.getName());
    }

    @Test
    public void testBuildNameWithoutQuotes() {
        SalaryReport salaryReport = builder.buildFromInputCsvFormat(
                "ADAMS CHRISTOPHER HENRY,GRADE 6 TEACHER,\"50,889.63\",\"1,817.35\",LBOE,ATLANTA INDEPENDENT SCHOOL SYSTEM,2010");

        assertEquals("name must be 'ADAMS CHRISTOPHER HENRY'", "ADAMS CHRISTOPHER HENRY", salaryReport.getName());
    }

    @Test
    public void testBuildTitleWithQuotes() {
        SalaryReport salaryReport = builder.buildFromInputCsvFormat(
                "\"ADAMS,CHRISTOPHER HENRY\",\"GRADE 6 TEACHER\",\"50,889.63\",\"1,817.35\",LBOE,ATLANTA INDEPENDENT SCHOOL SYSTEM,2010");

        assertEquals("title must be 'GRADE 6 TEACHER'", "GRADE 6 TEACHER", salaryReport.getTitle());

    }

    @Test
    public void testBuildTitleWithoutQuotes() {
        SalaryReport salaryReport = builder.buildFromInputCsvFormat(
                "\"ADAMS,CHRISTOPHER HENRY\",GRADE 6 TEACHER,\"50,889.63\",\"1,817.35\",LBOE,ATLANTA INDEPENDENT SCHOOL SYSTEM,2010");

        assertEquals("title must be 'GRADE 6 TEACHER'", "GRADE 6 TEACHER", salaryReport.getTitle());
    }

    @Test
    public void testBuildSalaryWithQuotesWithoutDollarsWithCommasWithoutSpaces() {
        SalaryReport salaryReport = builder.buildFromInputCsvFormat(
                "\"ADAMS,CHRISTOPHER HENRY\",\"GRADE 6 TEACHER\",\"50,889.63\",\"1,817.35\",LBOE,ATLANTA INDEPENDENT SCHOOL SYSTEM,2010");

        assertEquals("salary must be 50889.63", 50889.63f, salaryReport.getSalary());

    }

    @Test
    public void testBuildSalaryWithQuotesWithDollarsWithCommasWithSpaces() {
        SalaryReport salaryReport = builder.buildFromInputCsvFormat(
                "\"ADAMS,CHRISTOPHER HENRY\",\"GRADE 6 TEACHER\",\"$ 50,889.63 \",\"1,817.35\",LBOE,ATLANTA INDEPENDENT SCHOOL SYSTEM,2010");

        assertEquals("salary must be 50889.63", 50889.63f, salaryReport.getSalary());

    }

    @Test
    public void testBuildSalaryWithoutQuotesWithDollarsWithoutCommasWithoutSpaces() {
        SalaryReport salaryReport = builder.buildFromInputCsvFormat(
                "\"ADAMS,CHRISTOPHER HENRY\",\"GRADE 6 TEACHER\",$50889.63,\"1,817.35\",LBOE,ATLANTA INDEPENDENT SCHOOL SYSTEM,2010");

        assertEquals("salary must be 50889.63", 50889.63f, salaryReport.getSalary());

    }

    @Test
    public void testBuildTravelWithQuotesWithoutDollarsWithCommasWithoutSpaces() {
        SalaryReport salaryReport = builder.buildFromInputCsvFormat(
                "\"ADAMS,CHRISTOPHER HENRY\",\"GRADE 6 TEACHER\",\"50,889.63\",\"1,817.35\",LBOE,ATLANTA INDEPENDENT SCHOOL SYSTEM,2010");

        assertEquals("travel must be 1817.35", 1817.35f, salaryReport.getTravel());

    }

    @Test
    public void testBuildTravelWithoutQuotesWithDollarsWithoutCommasWithSpaces() {
        SalaryReport salaryReport = builder.buildFromInputCsvFormat(
                "\"ADAMS,CHRISTOPHER HENRY\",\"GRADE 6 TEACHER\",\"50,889.63\",$ 0.00,LBOE,ATLANTA INDEPENDENT SCHOOL SYSTEM,2010");

        assertEquals("travel must be 0.00", 0.00f, salaryReport.getTravel());

    }

    @Test
    public void testBuildTravelWithoutQuotesWithDollarsWithoutCommasWithSpacesNegativeValue() {
        SalaryReport salaryReport = builder.buildFromInputCsvFormat(
                "\"ADAMS,CHRISTOPHER HENRY\",\"GRADE 6 TEACHER\",\"50,889.63\",$- 500.00,LBOE,ATLANTA INDEPENDENT SCHOOL SYSTEM,2010");

        assertEquals("travel must be -500.00", -500.00f, salaryReport.getTravel());

    }

    //TODO test out the rest of the conversions, but... i'm feel GOOD!!

    @Test
    public void testBuildYearWithoutQuotes() {
        SalaryReport salaryReport = builder.buildFromInputCsvFormat(
                "\"ADAMS,CHRISTOPHER HENRY\",GRADE 6 TEACHER,\"50,889.63\",\"1,817.35\",LBOE,ATLANTA INDEPENDENT SCHOOL SYSTEM,2010");

        assertEquals("year must be 2010", 2010, salaryReport.getYear());
    }


}
