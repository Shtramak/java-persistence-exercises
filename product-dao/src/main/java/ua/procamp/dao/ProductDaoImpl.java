package ua.procamp.dao;

import ua.procamp.exception.DaoOperationException;
import ua.procamp.model.Product;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class ProductDaoImpl implements ProductDao {
    private static String SAVE_SQL = "INSERT INTO products(name, producer, price, expiration_date) VALUES (?,?,?,?)";
    private static String FIND_ALL_SQL = "SELECT * FROM products";
    private static String FIND_ONE_SQL = "SELECT * FROM products WHERE id = ?";
    private static String UPDATE_SQL = "UPDATE products SET name = ?, producer = ?, price = ?, expiration_date = ? WHERE id = ?";
    private static String REMOVE_SQL = "DELETE FROM products WHERE id = ?";

    private DataSource dataSource;

    public ProductDaoImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(Product product) {
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(SAVE_SQL, Statement.RETURN_GENERATED_KEYS);
            setStatementWithProduct(product, statement);
            statement.executeUpdate();
            ResultSet generatedKeys = statement.getGeneratedKeys();
            if (generatedKeys.next()) {
                long id = generatedKeys.getLong(1);
                product.setId(id);
            }
        } catch (SQLException e) {
            throw new DaoOperationException("Error saving product: " + product, e);
        }
    }

    private void setStatementWithProduct(Product product, PreparedStatement statement) throws SQLException {
        statement.setString(1, product.getName());
        statement.setString(2, product.getProducer());
        statement.setBigDecimal(3, product.getPrice());
        statement.setDate(4, Date.valueOf(product.getExpirationDate()));
    }

    @Override
    public List<Product> findAll() {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(FIND_ALL_SQL);
            List<Product> products = new ArrayList<>();
            while (resultSet.next()) {
                Product product = productFromResultSet(resultSet);
                products.add(product);
            }
            return products;
        } catch (SQLException e) {
            throw new DaoOperationException("Exception", e);
        }
    }

    private Product productFromResultSet(ResultSet resultSet) throws SQLException {
        return Product.builder()
                .id(resultSet.getLong("id"))
                .name(resultSet.getString("name"))
                .producer(resultSet.getString("producer"))
                .price(resultSet.getBigDecimal("price"))
                .expirationDate(resultSet.getDate("expiration_date").toLocalDate())
                .creationTime(resultSet.getTimestamp("creation_time").toLocalDateTime())
                .build();
    }

    @Override
    public Product findOne(Long id) {
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(FIND_ONE_SQL);
            statement.setLong(1, id);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return productFromResultSet(resultSet);
            } else {
                String message = String.format("Product with id = %d does not exist", id);
                throw new DaoOperationException(message);
            }
        } catch (SQLException e) {
            throw new DaoOperationException("Exception", e);
        }
    }

    @Override
    public void update(Product product) {
        verifyIndex(product.getId());
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(UPDATE_SQL);
            setStatementWithProduct(product, statement);
            statement.setLong(5, product.getId());
            executeUpdateAndVerify(statement, product);
        } catch (SQLException e) {
            throw new DaoOperationException("Exception", e);
        }
    }

    @Override
    public void remove(Product product) {
        verifyIndex(product.getId());
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(REMOVE_SQL);
            statement.setLong(1, product.getId());
            executeUpdateAndVerify(statement, product);
        } catch (SQLException e) {
            throw new DaoOperationException("Exception", e);
        }
    }

    private void executeUpdateAndVerify(PreparedStatement statement, Product product) throws SQLException {
        int rowsAffected = statement.executeUpdate();
        if (rowsAffected == 0) {
            String message = String.format("Product with id = %d does not exist", product.getId());
            throw new DaoOperationException(message);
        }
    }

    private void verifyIndex(Long id) {
        if (id == null) {
            throw new DaoOperationException("Product id cannot be null");
        }
    }

}
