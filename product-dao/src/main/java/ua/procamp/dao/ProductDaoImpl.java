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
    private static final String SAVE_QUERY =
            "INSERT INTO products(name, producer, price, expiration_date)" +
                    "VALUES (?,?,?,?)";

    private static final String FIND_ALL_QUERY = "SELECT * FROM products";

    private static final String FIND_ONE_QUERY = "SELECT * FROM products WHERE id = ?";

    private static final String UPDATE_QUERY =
            "UPDATE products " +
                    "SET name=?, producer=?,price=?, expiration_date=?" +
                    "WHERE id = ?";

    private static final String REMOVE_QUERY = "DELETE FROM products WHERE id=?";

    private DataSource dataSource;

    public ProductDaoImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(Product product) {
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(SAVE_QUERY, Statement.RETURN_GENERATED_KEYS);
            setProductToPreparedStatement(product, statement, true);
            statement.execute();
            ResultSet generatedKeys = statement.getGeneratedKeys();
            if (generatedKeys.next()) {
                long productId = generatedKeys.getLong(1);
                product.setId(productId);
            }
        } catch (SQLException e) {
            throw new DaoOperationException("Error saving product: " + product);
        }
    }

    @Override
    public List<Product> findAll() {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(FIND_ALL_QUERY);
            List<Product> products = new ArrayList<>();
            while (resultSet.next()) {
                Product product = productFromResultSet(resultSet);
                products.add(product);
            }
            return products;
        } catch (SQLException e) {
            throw new DaoOperationException("exception");
        }
    }

    @Override
    public Product findOne(Long id) {
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(FIND_ONE_QUERY);
            statement.setLong(1, id);
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            return productFromResultSet(resultSet);
        } catch (SQLException e) {
            String message = String.format("Product with id = %s does not exist", id);
            throw new DaoOperationException(message);
        }
    }

    @Override
    public void update(Product product) {
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(UPDATE_QUERY);
            setProductToPreparedStatement(product, statement, false);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DaoOperationException("Exception occurred during update...");
        }
    }

    @Override
    public void remove(Product product) {
        verifyId(product.getId());
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(REMOVE_QUERY);
            statement.setLong(1, product.getId());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DaoOperationException("Exception occurred during remove...");
        }
    }

    private void setProductToPreparedStatement(Product product, PreparedStatement statement, boolean isNew) throws SQLException {
        statement.setString(1, product.getName());
        statement.setString(2, product.getProducer());
        statement.setBigDecimal(3, product.getPrice());
        Date expirationDate = Date.valueOf(product.getExpirationDate());
        statement.setDate(4, expirationDate);
        if (!isNew) {
            verifyId(product.getId());
            statement.setLong(5, product.getId());
        }
    }

    private void verifyId(Long id) {
        if (id == null) {
            throw new DaoOperationException("Product id cannot be null");
        }
        if (id < 0) {
            String message = String.format("Product with id = %s does not exist", id);
            throw new DaoOperationException(message);
        }
    }

    private Product productFromResultSet(ResultSet resultSet) throws SQLException {
        Product product = new Product();
        product.setId(resultSet.getLong("id"));
        product.setName(resultSet.getString("name"));
        product.setProducer(resultSet.getString("producer"));
        product.setPrice(resultSet.getBigDecimal("price"));
        product.setExpirationDate(resultSet.getDate("expiration_date").toLocalDate());
        product.setCreationTime(resultSet.getTimestamp("creation_time").toLocalDateTime());
        return product;
    }
}
