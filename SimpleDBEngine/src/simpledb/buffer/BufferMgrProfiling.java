package simpledb.buffer;

import simpledb.jdbc.embedded.EmbeddedDriver;
import simpledb.server.SimpleDB;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

public class BufferMgrProfiling {

    public static Result createTableTest(int n, boolean mode) throws SQLException {
        EmbeddedDriver d = new EmbeddedDriver();
        String url = "jdbc:simpledb:studentdb" + UUID.randomUUID().toString().substring(9); //Makes new database each time
        Connection conn = d.connect(url, null);
        Statement stmt = conn.createStatement();

        // Debugging Setup
        SimpleDB db = d.getDb();
        BufferMgr bm = db.bufferMgr();
        bm.setMode(mode);

        String s = "create table STUDENT(SId int, SFirstName varchar(40), SLastName varchar(40), MajorId int, GradYear int)";
        stmt.executeUpdate(s);
        System.out.println("Table STUDENT created.");

        ArrayList<Name> names = Name.generateNames(n);

        s = "insert into STUDENT(SId, SFirstName, SLastName, MajorId, GradYear) values ";
        String[] studvals = new String[n];

        for (int i = 0; i < n; i++) {
            studvals[i] = String.format("(%d, '%s', '%s', %d, %d)", (i + 1), names.get(i).firstName,
                    names.get(i).lastName, rand.nextInt(courseNames.length), randomGradYear());
        }
        for (String studval : studvals) stmt.executeUpdate(s + studval);
        System.out.println("STUDENT records inserted.");

        conn.close();
        return new Result(bm.hits, bm.misses);
    }

    public static Result joinTableTest(int n, boolean mode) throws SQLException {
        EmbeddedDriver d = new EmbeddedDriver();
        String url = "jdbc:simpledb:studentdb" + UUID.randomUUID().toString().substring(9); //Makes new database each time
        Connection conn = d.connect(url, null);
        Statement stmt = conn.createStatement();

        // Debugging Setup
        SimpleDB db = d.getDb();
        BufferMgr bm = db.bufferMgr();
        bm.setMode(mode);

        String s = "create table STUDENT(SId int, SFirstName varchar(40), SLastName varchar(40), MajorId int, GradYear int)";
        stmt.executeUpdate(s);
        System.out.println("Table STUDENT created.");

        ArrayList<Name> names = Name.generateNames(n);

        s = "insert into STUDENT(SId, SFirstName, SLastName, MajorId, GradYear) values ";
        String[] studvals = new String[n];

        for (int i = 0; i < n; i++) {
            studvals[i] = String.format("(%d, '%s', '%s', %d, %d)", (i + 1), names.get(i).firstName,
                    names.get(i).lastName, rand.nextInt(courseNames.length), randomGradYear());
        }
        for (String studval : studvals) stmt.executeUpdate(s + studval);
        System.out.println("STUDENT records inserted.");

        s = "create table MAJOR(MId int, MajorName varchar(40), MajorAbbr varchar(5))";
        stmt.executeUpdate(s);
        System.out.println("Table MAJOR created.");

        s = "insert into MAJOR(MId, MajorName, MajorAbbr) values ";
        String[] majorvals = new String[courseNames.length];
        for (int i = 0; i < courseNames.length; i++) {
            majorvals[i] = String.format("(%d, '%s', '%s')", i, courseNames[i], courseAbs[i]);
        }
        for (String majorval : majorvals) stmt.executeUpdate(s + majorval);
        System.out.println("MAJOR records inserted.");

        bm.hits = 0;
        bm.misses = 0; // Resetting to only observe count during join

        s = "select SId, SFirstName, SLastName, MId, MajorName, MajorAbbr " +
                "from STUDENT, MAJOR " +
                "where MId = MajorId";

        ResultSet rs = stmt.executeQuery(s);
        int count = 0;
        while (rs.next()) {
            count++;
        } // Going through entire result set.

        conn.close();
        return new Result(bm.hits, bm.misses);
    }

    public static Result selectTableTest(int n, boolean mode) throws SQLException {
        EmbeddedDriver d = new EmbeddedDriver();
        String url = "jdbc:simpledb:studentdb" + UUID.randomUUID().toString().substring(9); //Makes new database each time
        Connection conn = d.connect(url, null);
        Statement stmt = conn.createStatement();

        // Debugging Setup
        SimpleDB db = d.getDb();
        BufferMgr bm = db.bufferMgr();
        bm.setMode(mode);

        String s = "create table STUDENT(SId int, SFirstName varchar(40), SLastName varchar(40), MajorId int, GradYear int)";
        stmt.executeUpdate(s);
        System.out.println("Table STUDENT created.");

        ArrayList<Name> names = Name.generateNames(n);

        s = "insert into STUDENT(SId, SFirstName, SLastName, MajorId, GradYear) values ";
        String[] studvals = new String[n];

        for (int i = 0; i < n; i++) {
            studvals[i] = String.format("(%d, '%s', '%s', %d, %d)", (i + 1), names.get(i).firstName,
                    names.get(i).lastName, rand.nextInt(courseNames.length), randomGradYear());
        }
        for (String studval : studvals) stmt.executeUpdate(s + studval);
        System.out.println("STUDENT records inserted.");

        bm.hits = 0;
        bm.misses = 0; // Resetting to only observe count during join

        s = "select SId, SFirstName, SLastName, MajorId " +
                "from STUDENT";

        ResultSet rs = stmt.executeQuery(s);
        int count = 0;
        while (rs.next()) {
            count++;
        } // Going through entire result set.

        conn.close();
        return new Result(bm.hits, bm.misses);
    }

    /**
     * Specifies size of experiment
     */
    final static int[] nExperiments = new int[]{20, 40, 60, 80, 100, 140, 180, 220, 260, 300, 350, 400, 450, 500, 550,
            600, 650, 750, 800, 850, 900, 950, 1000};

    /**
     * Tests performance while Creating Tables of various sizes, specified by nExperiments
     * Performance of buffers is evaluated by hits and misses
     */
    public static void createTableTests() throws FileNotFoundException, SQLException {
        PrintWriter pw = new PrintWriter(new File("createTable.csv"));
        pw.println("mode,n,hits,misses,total");
        for (int n : nExperiments) {
            pw.println(createTableTest(n, true).toString("MRU", n));
            pw.println(createTableTest(n, false).toString("default", n));
        }
        pw.close();
    }

    /**
     * Tests performance while joining Tables of various sizes, specified by nExperiments, with a constant size table.
     * Performance of buffers is evaluated by hits and misses
     */
    public static void joinTableTests() throws FileNotFoundException, SQLException {
        PrintWriter pw = new PrintWriter(new File("joinTable.csv"));
        pw.println("mode,n,hits,misses,total");
        for (int n : nExperiments) {
            pw.println(joinTableTest(n, true).toString("MRU", n));
            pw.println(joinTableTest(n, false).toString("default", n));
        }
        pw.close();
    }

    /**
     * Tests performance while joining Tables of various sizes, specified by nExperiments, with a constant size table.
     * Performance of buffers is evaluated by hits and misses
     */
    public static void selectTableTests() throws FileNotFoundException, SQLException {
        PrintWriter pw = new PrintWriter(new File("selectTable.csv"));
        pw.println("mode,n,hits,misses,total");
        for (int n : nExperiments) {
            pw.println(selectTableTest(n, true).toString("MRU", n));
            pw.println(selectTableTest(n, false).toString("default", n));
        }
        pw.close();
    }

    public static void main(String[] args) {
        try {
//            createTableTests();
//            joinTableTests();
            selectTableTests();
        } catch (SQLException | FileNotFoundException throwables) {
            throwables.printStackTrace();
        }
    }

    /* HELPER FUNCTIONS */

    private static class Result {
        int hits;
        int misses;
        int total;

        public Result(int hits, int misses) {
            this.hits = hits;
            this.misses = misses;
            this.total = hits + misses;
        }

        public String toString(String expName, int n) {
            return String.format("%s,%d,%d,%d,%d", expName, n, hits, misses, total);
        }

        @Override
        public String toString() {
            return String.format("%d,%d,%d", hits, misses, total);
        }
    }

    private static final Random rand = new Random(42); // For number generation

    private static String randomGrade() {
        String grade = "ABCDF".charAt(rand.nextInt(5)) + "";
        switch (rand.nextInt(3)) {
            case 0:
                grade += "-";
                break;
            case 1:
                grade += "+";
        }
        return grade;
    }

    private static int randomGradYear() {
        return 2021 + rand.nextInt(4);
    }

    private static final String[] courseNames = new String[]{"Computer Science", "Chemical Engineering",
            "Mechanical Engineering", "Aerospace Engineering", "Computer Engineering", "Electrical Engineering",
            "Environmental Engineering", "Biomedical Engineering", "Biology", "Physics", "Chemistry", "English",
            "Psychology", "Economics", "Management", "Statistics", "French", "German", "Civil Engineering", "Art Design"};

    private static final String[] courseAbs = new String[]{"CS", "CHE", "ME", "ASE", "ECE", "EE", "EEE", "BME", "BIO",
            "PHY", "CHM", "ENG", "PSY", "ECON", "MGMT", "STAT", "FRE", "GER", "CE", "ART"};

    private static class Name {
        String firstName;
        String lastName;

        public Name(String firstName, String lastName) {
            this.firstName = firstName;
            this.lastName = lastName;
        }

        public static ArrayList<Name> generateNames(int n) {
            ArrayList<Name> names = new ArrayList<>();
            int c1 = 0;
            int c2 = 0;
            n = Math.min(n, firstNameList.length * lastNameList.length);
            for (int i = 0; i < n; i++) {
                if (c1 == firstNameList.length) {
                    c1 = 0;
                    c2++;
                }
                names.add(new Name(firstNameList[c1++], lastNameList[c2]));
            }
            return names;
        }

        static final String[] firstNameList = new String[]{"Adam", "Adrian", "Alan", "Alexander", "Andrew", "Anthony", "Austin",
                "Benjamin", "Blake", "Boris", "Brandon", "Brian", "Cameron", "Carl", "Charles", "Christian", "Christopher",
                "Colin", "Connor", "Dan", "David", "Dominic", "Dylan", "Edward", "Eric", "Evan", "Frank", "Gavin", "Gordon",
                "Harry", "Ian", "Isaac", "Jack", "Jacob", "Jake", "James", "Jason", "Joe", "John", "Jonathan", "Joseph",
                "Joshua", "Julian", "Justin", "Keith", "Kevin", "Leonard", "Liam", "Lucas", "Luke", "Matt", "Max",
                "Michael", "Nathan", "Neil", "Nicholas", "Oliver", "Owen", "Paul", "Peter", "Phil", "Piers",
                "Richard", "Robert", "Ryan", "Sam", "Sean", "Sebastian", "Simon", "Stephen", "Steven",
                "Stewart", "Thomas", "Tim", "Trevor", "Victor", "Warren", "William"};

        static final String[] lastNameList = new String[]{"Abraham", "Allan", "Alsop", "Anderson", "Arnold", "Avery", "Bailey",
                "Baker", "Ball", "Bell", "Berry", "Black", "Blake", "Bond", "Bower", "Brown", "Buckland", "Burgess",
                "Butler", "Cameron", "Campbell", "Carr", "Chapman", "Churchill", "Clark", "Clarkson", "Coleman", "Cornish",
                "Davidson", "Davies", "Dickens", "Dowd", "Duncan", "Dyer", "Edmunds", "Ellison", "Ferguson", "Fisher",
                "Forsyth", "Fraser", "Gibson", "Gill", "Glover", "Graham", "Grant", "Gray", "Greene", "Hamilton",
                "Hardacre", "Harris", "Hart", "Hemmings", "Henderson", "Hill", "Hodges", "Howard", "Hudson", "Hughes",
                "Hunter", "Ince", "Jackson", "James", "Johnston", "Jones", "Kelly", "Kerr", "King", "Knox", "Lambert",
                "Langdon", "Lawrence", "Lee", "Lewis", "Lyman", "MacDonald", "Mackay", "Mackenzie", "MacLeod", "Manning",
                "Marshall", "Martin", "Mathis", "May", "McDonald", "McLean", "McGrath", "Metcalfe", "Miller", "Mills",
                "Mitchell", "Morgan", "Morrison", "Murray", "Nash", "Newman", "Nolan", "North", "Ogden", "Oliver", "Paige",
                "Parr", "Parsons", "Paterson", "Payne", "Peake", "Peters", "Piper", "Poole", "Powell", "Pullman", "Quinn",
                "Rampling", "Randall", "Rees", "Reid", "Roberts", "Robertson", "Ross", "Russell", "Rutherford", "Sanderson",
                "Scott", "Sharp", "Short", "Simpson", "Skinner", "Slater", "Smith", "Springer", "Stewart", "Sutherland",
                "Taylor", "Terry", "Thomson", "Tucker", "Turner", "Underwood", "Vance", "Vaughan", "Walker", "Wallace",
                "Walsh", "Watson", "Welch", "White", "Wilkins", "Wilson", "Wright", "Young"};
    }
}
