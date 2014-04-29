package lestermartin.hadoop.exploration.opengeorgia;


public class SalaryReport {

    private String name;
    private String title;
    private float salary;
    private float travel;
    private String orgType;
    private String org;
    private int year;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public float getTravel() {
        return travel;
    }

    public void setTravel(float travel) {
        this.travel = travel;
    }

    public String getOrgType() {
        return orgType;
    }

    public void setOrgType(String orgType) {
        this.orgType = orgType;
    }

    public String getOrg() {
        return org;
    }

    public void setOrg(String org) {
        this.org = org;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public float getSalary() {
        return salary;
    }

    public void setSalary(float salary) {
        this.salary = salary;
    }

    public boolean equals(SalaryReport reportToCompare) {
        if( ! reportToCompare.getName().equals(this.getName()) ) {
            return false;
        } else if( ! reportToCompare.getTitle().equals(this.getTitle()) ) {
            return false;
        } else if( reportToCompare.getSalary() != this.getSalary() ) {
            return false;
        } else if( reportToCompare.getTravel() != this.getTravel() ) {
            return false;
        } else if( ! reportToCompare.getOrgType().equals(this.getOrgType()) ) {
            return false;
        } else if( ! reportToCompare.getOrg().equals(this.getOrg()) ) {
            return false;
        } else if( reportToCompare.getYear() != this.getYear() )  {
            return false;
        } else {
            return true;
        }

    }


}
