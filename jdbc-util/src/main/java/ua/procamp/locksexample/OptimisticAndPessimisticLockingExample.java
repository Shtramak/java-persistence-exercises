package ua.procamp.locksexample;

import ua.procamp.Program;
import ua.procamp.locksexample.exception.OptimisticLockingException;
import ua.procamp.util.JdbcUtil;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class OptimisticAndPessimisticLockingExample {
    private static final String SELECT_PROGRAM_SQL = "SELECT * FROM programs WHERE id = ?";
    private static final String SELECT_WITH_LOCK_PROGRAM_SQL = "SELECT * FROM programs WHERE id = ? FOR UPDATE";
    private static final String UPDATE_PROGRAM_OPTIMISTIC_SQL =
            "UPDATE programs SET name = ?, description = ?, version = ?  WHERE id = ? AND version=?";
    private static final String UPDATE_PROGRAM_PESIMISTIC_SQL =
            "UPDATE programs SET name = ?, description = ?, version = ?  WHERE id = ?";
    private static DataSource dataSource;

    public static void main(String[] args) {
        dataSource = JdbcUtil.createPostgresDataSource("jdbc:postgresql://localhost:5432/test_db", "postgres", "postgres");
        try {
            Program program = Program.builder()
                    .name("pesimistic update")
                    .description("pesimistic update")
                    .build();
//            updateWithOptimisticLocking(2L, program);
            updateWithPesimisticLocking(3L, program);
        } catch (SQLException e) {
            System.out.println("Shit happens...");
        }

    }


    private static void updateWithPesimisticLocking(Long programId, Program program) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            PreparedStatement statement = connection.prepareStatement(SELECT_WITH_LOCK_PROGRAM_SQL);
            statement.setLong(1, programId);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                Program currentProgram = programFromResultSet(resultSet);
                System.out.println(currentProgram);
                try {
                    preparePessimisticUpdateAndCommit(connection, currentProgram, program);
                } catch (OptimisticLockingException e) {
                    connection.rollback();
                    System.out.println("Update fails...");
                }
            } else {
                System.out.printf("No program with %d found", programId);
                connection.rollback();
            }
        }
    }

    private static void updateWithOptimisticLocking(Long programId, Program program) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            PreparedStatement statement = connection.prepareStatement(SELECT_PROGRAM_SQL);
            statement.setLong(1, programId);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                Program currentProgram = programFromResultSet(resultSet);
                System.out.println(currentProgram);
                try {
                    prepareOptimisticUpdateAndCommit(connection, currentProgram, program);
                } catch (OptimisticLockingException e) {
                    connection.rollback();
                    System.out.println("Update fails...");
                }
            } else {
                System.out.printf("No program with %d found", programId);
                connection.rollback();
            }
        }
    }

    private static void prepareOptimisticUpdateAndCommit(Connection connection, Program currentProgram, Program program) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(UPDATE_PROGRAM_OPTIMISTIC_SQL);
        program.setVersion(currentProgram.getVersion() + 1);
        program.setId(currentProgram.getId());
        prepareStatementForUpdate(statement, program, true);
        int rowsAffected = statement.executeUpdate();
        if (rowsAffected != 0) {
            connection.commit();
            System.out.println("Updated successfully");
            System.out.println("New program: " + program);
        } else {
            throw new OptimisticLockingException("Update fails ...");
        }
    }

    private static void preparePessimisticUpdateAndCommit(Connection connection, Program currentProgram, Program program) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(UPDATE_PROGRAM_PESIMISTIC_SQL);
        program.setVersion(currentProgram.getVersion() + 1);
        program.setId(currentProgram.getId());
        prepareStatementForUpdate(statement, program, false);
        int rowsAffected = statement.executeUpdate();
        if (rowsAffected != 0) {
            connection.commit();
            System.out.println("Updated successfully");
            System.out.println("New program: " + program);
        } else {
            throw new RuntimeException("Update fails ...");
        }
    }

    private static void prepareStatementForUpdate(PreparedStatement statement, Program program, boolean isOptimistic) throws SQLException {
        statement.setString(1, program.getName());
        statement.setString(2, program.getDescription());
        statement.setInt(3, program.getVersion());
        statement.setLong(4, program.getId());
        if (isOptimistic) {
            statement.setInt(5, program.getVersion() - 1);
        }
    }

    private static Program programFromResultSet(ResultSet resultSet) throws SQLException {
        return Program.builder()
                .id(resultSet.getLong("id"))
                .name(resultSet.getString("name"))
                .description(resultSet.getString("description"))
                .version(resultSet.getInt("version"))
                .build();
    }
}

/*
start tx
read program by id
do some logic..
update program using optimistic locking
commit
or throw OptimisticLockingException
*/