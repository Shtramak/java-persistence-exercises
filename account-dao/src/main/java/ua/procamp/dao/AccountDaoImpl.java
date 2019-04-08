package ua.procamp.dao;

import ua.procamp.exception.AccountDaoException;
import ua.procamp.model.Account;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class AccountDaoImpl implements AccountDao {
    private EntityManagerFactory emf;

    public AccountDaoImpl(EntityManagerFactory emf) {
        this.emf = emf;
    }

    @Override
    public void save(Account account) {
        executeWithinTransaction(entityManager -> entityManager.persist(account));
    }

    @Override
    public Account findById(Long id) {
        return executeWithinTransactionReturningResult(entityManager -> entityManager.find(Account.class, id));
    }

    @Override
    public Account findByEmail(String email) {
        return executeWithinTransactionReturningResult(entityManager ->
                entityManager.createQuery("SELECT a FROM Account a WHERE a.email=:email", Account.class)
                        .setParameter("email", email)
                        .getSingleResult());
    }

    @Override
    public List<Account> findAll() {
        return executeWithinTransactionReturningResult(entityManager ->
                entityManager.createQuery("from Account", Account.class)
                        .getResultList());
    }

    @Override
    public void update(Account account) {
        executeWithinTransaction(entityManager -> entityManager.merge(account));
    }

    @Override
    public void remove(Account account) {
        executeWithinTransaction(entityManager -> {
            Account managedAccount = entityManager.merge(account);
            entityManager.remove(managedAccount);
        });
    }

    private void executeWithinTransaction(Consumer<EntityManager> emConsumer) {
        EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();
        try {
            emConsumer.accept(entityManager);
            entityManager.getTransaction().commit();
        } catch (Exception e) {
            entityManager.getTransaction().rollback();
            throw new AccountDaoException("Exception occurred while executing transaction", e);
        } finally {
            entityManager.close();
        }
    }

    private <T> T executeWithinTransactionReturningResult(Function<EntityManager, T> emFunction) {
        EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();
        try {
            T result = emFunction.apply(entityManager);
            entityManager.getTransaction().commit();
            return result;
        } catch (Exception e) {
            entityManager.getTransaction().rollback();
            throw new AccountDaoException("Exception occurred while executing transaction", e);
        } finally {
            entityManager.close();
        }
    }
}

