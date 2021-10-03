package simpledb.buffer;

import simpledb.jdbc.embedded.EmbeddedDriver;
import simpledb.server.SimpleDB;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;

public class BufferMgrProfiling {
    
    final static int BLOCK_SIZE = 50;
    final static int BUF_SIZE = 3;
    
    public static Result createTableTest(int n, boolean mode) throws SQLException {
        EmbeddedDriver d = new EmbeddedDriver();
        String url = "jdbc:simpledb:studentdb" + UUID.randomUUID().toString().substring(9); //Makes new database each time
        Connection conn = d.connect(url, BLOCK_SIZE, BUF_SIZE, null);
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
        Connection conn = d.connect(url, BLOCK_SIZE, BUF_SIZE, null);
        Statement stmt = conn.createStatement();

        // Debugging Setup
        SimpleDB db = d.getDb();
        BufferMgr bm = db.bufferMgr();
        bm.setMode(mode);

        String s = "create table STUDENT(SId int, SFirstName varchar(40), " +
                "SLastName varchar(40), MajorId int, GradYear int)";
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
        BufferMgr.discWrites = 0;

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
        String url = "jdbc:simpledb:studentdb" + UUID.randomUUID().toString().substring(9);
        //Makes new database each time
        Connection conn = d.connect(url, BLOCK_SIZE, BUF_SIZE, null);
        Statement stmt = conn.createStatement();

        // Debugging Setup
        SimpleDB db = d.getDb();
        BufferMgr bm = db.bufferMgr();
        bm.setMode(mode);

        String s = "create table STUDENT(SId int, SFirstName varchar(40), " +
                "SLastName varchar(40), MajorId int, GradYear int)";
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
        BufferMgr.discWrites = 0;

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

    public static Result largeJoinTableTest(int n, boolean mode) throws SQLException {
        EmbeddedDriver d = new EmbeddedDriver();
        String url = "jdbc:simpledb:studentdb" + UUID.randomUUID().toString().substring(9); //Makes new database each time
        Connection conn = d.connect(url, BLOCK_SIZE, BUF_SIZE, null);
        Statement stmt = conn.createStatement();

        // Debugging Setup
        SimpleDB db = d.getDb();
        BufferMgr bm = db.bufferMgr();
        bm.setMode(mode);

        String s = "create table STUDENT(SId int, SFirstName varchar(40), " +
                "SLastName varchar(40), MajorId int, GradYear int)";
        stmt.executeUpdate(s);
        System.out.println("Table STUDENT created.");

        ArrayList<Name> names = Name.generateNames(n);

        s = "insert into STUDENT(SId, SFirstName, SLastName, MajorId, GradYear) values ";
        String[] studvals = new String[n / 2];

        for (int i = 0; i < n / 2; i++) {
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

        s = "create table INSTRUCTOR(IId int, IFirstName varchar(40), " +
                "ILastName varchar(40), DeptID int)";
        stmt.executeUpdate(s);
        System.out.println("Table INSTRUCTOR created.");

        s = "insert into INSTRUCTOR(IId, IFirstName, ILastName, DeptID) values ";
        String[] instrVals = new String[n / 2];

        for (int i = n / 2; i < n; i++) {
            instrVals[i - n / 2] = String.format("(%d, '%s', '%s', %d, %d)", (i + 1), names.get(i).firstName,
                    names.get(i).lastName, rand.nextInt(courseNames.length), randomGradYear());
        }
        for (String instrVal : instrVals) stmt.executeUpdate(s + instrVal);
        System.out.println("INSTRUCTOR records inserted.");

        bm.hits = 0;
        bm.misses = 0; // Resetting to only observe count during join
        BufferMgr.discWrites = 0;

        s = "select SId, SFirstName, SLastName, MId, MajorName, MajorAbbr, IID, IFirstName, ILastName, DeptID " +
                "from STUDENT, MAJOR, INSTRUCTOR " +
                "where MId = MajorId and DeptID = MId";

        ResultSet rs = stmt.executeQuery(s);
        int count = 0;
        while (rs.next()) {
            count++;
        } // Going through entire result set.

        conn.close();
        return new Result(bm.hits, bm.misses);
    }

    public static Result updateTableTest(int n, boolean mode) throws SQLException {
        EmbeddedDriver d = new EmbeddedDriver();
        String url = "jdbc:simpledb:studentdb" + UUID.randomUUID().toString().substring(9);
        //Makes new database each time
        Connection conn = d.connect(url, BLOCK_SIZE, BUF_SIZE, null);
        Statement stmt = conn.createStatement();

        // Debugging Setup
        SimpleDB db = d.getDb();
        BufferMgr bm = db.bufferMgr();
        bm.setMode(mode);

        String s = "create table STUDENT(SId int, SFirstName varchar(40), " +
                "SLastName varchar(40), MajorId int, GradYear int)";
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
        BufferMgr.discWrites = 0;

        for (int i = 0; i < n * 0.75; i++) {
            String cmd = "update STUDENT "
                    + "set MajorId=" + rand.nextInt(courseNames.length) + " "
                    + "where SId = " + rand.nextInt(n) + 1;
            stmt.executeUpdate(cmd);
        }

        conn.close();
        return new Result(bm.hits, bm.misses);
    }

    public static Result deleteFromTableTest(int n, boolean mode) throws SQLException {
        EmbeddedDriver d = new EmbeddedDriver();
        String url = "jdbc:simpledb:studentdb" + UUID.randomUUID().toString().substring(9);
        //Makes new database each time
        Connection conn = d.connect(url, BLOCK_SIZE, BUF_SIZE, null);
        Statement stmt = conn.createStatement();

        // Debugging Setup
        SimpleDB db = d.getDb();
        BufferMgr bm = db.bufferMgr();
        bm.setMode(mode);

        String s = "create table STUDENT(SId int, SFirstName varchar(40), " +
                "SLastName varchar(40), MajorId int, GradYear int)";
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
        BufferMgr.discWrites = 0;

        for (int i = 0; i < n * 0.25; i++) {
            int index = rand.nextInt(names.size());
            s = "delete from STUDENT "
                    + "where SFirstName = '" + names.get(index).firstName + "'";
            names.remove(index);
            stmt.executeUpdate(s);
        }

        // Larger Removals

        s = "delete from STUDENT "
                + "where GradYear = " + randomGradYear();
        stmt.executeUpdate(s);
        conn.close();
        return new Result(bm.hits, bm.misses);
    }

    public static Result randomizedTest(int n, boolean mode) throws SQLException {
        EmbeddedDriver d = new EmbeddedDriver();
        String url = "jdbc:simpledb:studentdb" + UUID.randomUUID().toString().substring(9);
        //Makes new database each time
        Connection conn = d.connect(url, BLOCK_SIZE, BUF_SIZE, null);
        Statement stmt = conn.createStatement();

        // Debugging Setup
        SimpleDB db = d.getDb();
        BufferMgr bm = db.bufferMgr();
        bm.setMode(mode);

        String s = "create table STUDENT(SId int, SFirstName varchar(40), " +
                "SLastName varchar(40), MajorId int, GradYear int)";
        stmt.executeUpdate(s);
        System.out.println("Table STUDENT created.");

        ArrayList<Name> names = Name.generateNames(3 * n);

        s = "insert into STUDENT(SId, SFirstName, SLastName, MajorId, GradYear) values ";
        String[] studvals = new String[n];

        ArrayList<Integer> ids = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            studvals[i] = String.format("(%d, '%s', '%s', %d, %d)", (i + 1), names.get(0).firstName,
                    names.get(0).lastName, rand.nextInt(courseNames.length), randomGradYear());
            names.remove(0);
            ids.add((i + 1));
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
        bm.misses = 0; // Resetting to only observe count during random tests now
        BufferMgr.discWrites = 0;

        int id = 0;
        String cmd = "";

        for (int i = 0; i < 2 * n; i++) {
            switch (rand.nextInt(5)) {
                case 0: // Insertion
                    s = "insert into STUDENT(SId, SFirstName, SLastName, MajorId, GradYear) values " +
                            String.format("(%d, '%s', '%s', %d, %d)", ids.get(ids.size() - 1) + 1, names.get(0).firstName,
                                    names.get(0).lastName, rand.nextInt(courseNames.length), randomGradYear());
                    names.remove(0);
                    stmt.executeUpdate(s);
                    ids.add(ids.get(ids.size() - 1) + 1);
                    break;
                case 1: // Update
                    id = ids.get(rand.nextInt(ids.size()));
                    cmd = "update STUDENT "
                            + "set MajorId=" + rand.nextInt(courseNames.length) + " "
                            + "where SId = " + id;
                    stmt.executeUpdate(cmd);
                    break;
                case 2: // Selection
                    id = ids.get(rand.nextInt(ids.size()));
                    s = "select SId, SFirstName, SLastName " +
                            "from STUDENT " +
                            "where SId = " + id;

                    ResultSet rs = stmt.executeQuery(s);
                    int count = 0;
                    while (rs.next()) {
                        count++;
                    } // Going through entire result set.
                    break;
                case 3: // Deletion
                    id = ids.get(rand.nextInt(ids.size()));
                    ids.remove(Integer.valueOf(id));
                    cmd = "delete from STUDENT "
                            + "where SId = " + id;
                    stmt.executeUpdate(cmd);
                    break;
                case 4: // Join
                    id = ids.get(rand.nextInt(ids.size()));
                    s = "select SId, SFirstName, SLastName, MId, MajorName, MajorAbbr " +
                            "from STUDENT, MAJOR " +
                            "where SId = " + id;
                    ResultSet rs1 = stmt.executeQuery(s);
                    int count1 = 0;
                    while (rs1.next()) {
                        count1++;
                    } // Going through entire result set.
            }
        }

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
        PrintWriter pw = new PrintWriter("createTable.csv");
        pw.println("mode,n,hits,misses,total,writes");
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
        PrintWriter pw = new PrintWriter("joinTable.csv");
        pw.println("mode,n,hits,misses,total,writes");
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
        PrintWriter pw = new PrintWriter("selectTable.csv");
        pw.println("mode,n,hits,misses,total,writes");
        for (int n : nExperiments) {
            pw.println(selectTableTest(n, true).toString("MRU", n));
            pw.println(selectTableTest(n, false).toString("default", n));
        }
        pw.close();
    }

    /**
     * Tests performance while joining two tables of various sizes, specified by nExperiments, with a constant size table.
     * Performance of buffers is evaluated by hits and misses
     */
    public static void largeJoinTableTests() throws FileNotFoundException, SQLException {
        PrintWriter pw = new PrintWriter("largeJoinTable.csv");
        pw.println("mode,n,hits,misses,total,writes");
        for (int n : nExperiments) {
            pw.println(largeJoinTableTest(n, true).toString("MRU", n));
            pw.println(largeJoinTableTest(n, false).toString("default", n));
        }
        pw.close();
    }

    /**
     * Tests performance while updating values in tables of various sizes
     */
    public static void updateTableTests() throws FileNotFoundException, SQLException {
        PrintWriter pw = new PrintWriter("updateTable.csv");
        pw.println("mode,n,hits,misses,total,writes");
        for (int n : nExperiments) {
            pw.println(updateTableTest(n, true).toString("MRU", n));
            pw.println(updateTableTest(n, false).toString("default", n));
        }
        pw.close();
    }

    /**
     * Tests performance while removing values in tables of various sizes
     */
    public static void deleteFromTableTests() throws FileNotFoundException, SQLException {
        PrintWriter pw = new PrintWriter("deleteFromTable.csv");
        pw.println("mode,n,hits,misses,total,writes");
        for (int n : nExperiments) {
            pw.println(deleteFromTableTest(n, true).toString("MRU", n));
            pw.println(deleteFromTableTest(n, false).toString("default", n));
        }
        pw.close();
    }

    /**
     * Tests performance in randomized tasks
     */
    public static void randomizedTests() throws FileNotFoundException, SQLException {
        PrintWriter pw = new PrintWriter("randomTests.csv");
        pw.println("mode,n,hits,misses,total,writes");
        for (int n : nExperiments) {
            pw.println(randomizedTest(n, true).toString("MRU", n));
            pw.println(randomizedTest(n, false).toString("default", n));
        }
        pw.close();
    }

    public static void main(String[] args) {
        try {
            createTableTests();
            joinTableTests();
            selectTableTests();
            largeJoinTableTests();
            updateTableTests();
            deleteFromTableTests();
            randomizedTests();
        } catch (SQLException | FileNotFoundException throwables) {
            throwables.printStackTrace();
        }
    }

    /* HELPER FUNCTIONS */

    private static class Result {
        int hits;
        int misses;
        int total;
        int discWrites;

        public Result(int hits, int misses) {
            this.hits = hits;
            this.misses = misses;
            this.total = hits + misses;
            this.discWrites = BufferMgr.discWrites;
        }

        public String toString(String expName, int n) {
            return String.format("%s,%d,%d,%d,%d,%d", expName, n, hits, misses, total, discWrites);
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Name name = (Name) o;
            return Objects.equals(firstName, name.firstName) && Objects.equals(lastName, name.lastName);
        }

        public static ArrayList<Name> generateNames(int n) {
            ArrayList<Name> names = new ArrayList<>();
            int c1 = 0;
            int c2 = 0;
            n = Math.min(n, firstNameList.length * lastNameList.length);
            for (int i = 0; i < n; i++) {
                Name name = new Name(firstNameList[rand.nextInt(firstNameList.length)],
                        lastNameList[rand.nextInt(lastNameList.length)]);
                while (names.contains(name)) {
                    name = new Name(firstNameList[rand.nextInt(firstNameList.length)],
                            lastNameList[rand.nextInt(lastNameList.length)]);
                }
                names.add(name);
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
