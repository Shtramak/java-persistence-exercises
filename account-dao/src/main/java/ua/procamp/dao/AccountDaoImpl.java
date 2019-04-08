package ua.procamp.dao;

import ua.procamp.exception.AccountDaoException;
import ua.procamp.model.Account;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import java.util.List;

public class AccountDaoImpl implements AccountDao {
    private EntityManagerFactory emf;

    public AccountDaoImpl(EntityManagerFactory emf) {
        this.emf = emf;
    }

    @Override
    public void save(Account account) {
        EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();
        try {
            entityManager.persist(account);
            entityManager.getTransaction().commit();
        } catch (Exception e) {
            entityManager.getTransaction().rollback();
            throw new AccountDaoException("Exception", e);
        } finally {
            entityManager.close();
        }
    }

    @Override
    public Account findById(Long id) {
        EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();
        try {
            Account account = entityManager.find(Account.class, id);
            entityManager.getTransaction().commit();
            return account;
        } catch (Exception e) {
            entityManager.getTransaction().rollback();
            throw new AccountDaoException("Exception", e);
        } finally {
            entityManager.close();
        }

    }

    @Override
    public Account findByEmail(String email) {
        EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();
        try {
            return entityManager
                    .createQuery("SELECT a FROM Account a WHERE a.email=:email", Account.class)
                    .setParameter("email", email)
                    .getSingleResult();
        } catch (Exception e) {
            entityManager.getTransaction().rollback();
            throw new AccountDaoException("Exception", e);
        } finally {
            entityManager.close();
        }
    }

    @Override
    public List<Account> findAll() {
        EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();
        try {
            return entityManager.createQuery("from Account", Account.class).getResultList();
        } catch (Exception e) {
            entityManager.getTransaction().rollback();
            throw new AccountDaoException("Exception", e);
        } finally {
            entityManager.close();
        }
    }

    @Override
    public void update(Account account) {
        EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();
        try {
            entityManager.merge(account);
            entityManager.getTransaction().commit();
        } catch (Exception e) {
            entityManager.getTransaction().rollback();
            throw new AccountDaoException("Exception", e);
        } finally {
            entityManager.close();
        }
    }

    @Override
    public void remove(Account account) {
        EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();
        try {
            Account managedAccount = entityManager.merge(account);
            entityManager.remove(managedAccount);
            entityManager.getTransaction().commit();
        } catch (Exception e) {
            entityManager.getTransaction().rollback();
            throw new AccountDaoException("Exception", e);
        } finally {
            entityManager.close();
        }
    }
}

