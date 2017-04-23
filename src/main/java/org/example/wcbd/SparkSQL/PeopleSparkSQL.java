package org.example.wcbd.SparkSQL;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class PeopleSparkSQL {

    public static class Person implements Serializable {

        private String firstName;
        private String lastName;
        private String companyName;
        private String address;
        private String city;
        private String county;
        private String postal;
        private String phone1;
        private String phone2;
        private String email;
        private String web;

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        public String getCompanyName() {
            return companyName;
        }

        public void setCompanyName(String companyName) {
            this.companyName = companyName;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public String getCounty() {
            return county;
        }

        public void setCounty(String county) {
            this.county = county;
        }

        public String getPostal() {
            return postal;
        }

        public void setPostal(String postal) {
            this.postal = postal;
        }

        public String getPhone1() {
            return phone1;
        }

        public void setPhone1(String phone1) {
            this.phone1 = phone1;
        }

        public String getPhone2() {
            return phone2;
        }

        public void setPhone2(String phone2) {
            this.phone2 = phone2;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public String getWeb() {
            return web;
        }

        public void setWeb(String web) {
            this.web = web;
        }
    }

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("PeopleSparkSQL");
        // -----
        SparkSession spark  = SparkSession.builder()
                .appName("PeopleSparkSQL")
                .master("local[*]")
                .config("option", "some-value")
                .getOrCreate();

        SparkContext sc = spark.sparkContext();
//        SQLContext sqlContext = spark.sqlContext();

        JavaSparkContext ctx = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(ctx);

        System.out.println("=== Data source: RDD ===");
        // Load a text file and convert each line to a Java Bean.
        JavaRDD<Person> people = ctx.textFile("/Users/ritesh/Documents/DataScience/advanceBigData/ukdata_cleaned.txt").map(
                new Function<String, Person>() {
                    @Override
                    public Person call(String line) {
                        String[] parts = line.split(",");

                        Person person = new Person();

                        person.setFirstName(parts[0].trim());
                        person.setLastName (parts[1].trim());
                        person.setCompanyName(parts[2].trim());
                        person.setAddress (parts[3].trim());
                        person.setCity(parts[4].trim());
                        person.setCounty(parts[5].trim());
                        person.setPostal(parts[6].trim());
                        person.setPhone1(parts[7].trim());
                        person.setPhone2(parts[8].trim());
                        person.setEmail(parts[9].trim());
                        person.setWeb(parts[10].trim());


                        return person;
                    }
                });

        // Apply a schema to an RDD of Java Beans and register it as a table.
        Dataset schemaPeople = sqlContext.createDataFrame(people, Person.class);
        schemaPeople.registerTempTable("people");

        // SQL can be run over RDDs that have been registered as tables.
        Dataset gmailUsers = sqlContext.sql("SELECT firstName FROM people ");

        // The results of SQL queries are DataFrames and support all the normal RDD operations.
        // The columns of a row in the result can be accessed by ordinal.
        List<String> gmailUserNames = gmailUsers.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) {
                return "Name: " + row.getString(0);
            }
        }).collect();
        for (String name: gmailUserNames) {
            System.out.println(name);
        }

        ctx.stop();
    }
}
