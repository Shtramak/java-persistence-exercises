package ua.procamp.dao;

import org.hibernate.Session;
import ua.procamp.exception.CompanyDaoException;
import ua.procamp.model.Company;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import java.util.function.Function;

public class CompanyDaoImpl implements CompanyDao {
    private EntityManagerFactory entityManagerFactory;

    public CompanyDaoImpl(EntityManagerFactory entityManagerFactory) {
        this.entityManagerFactory = entityManagerFactory;
    }

    @Override
    public Company findByIdFetchProducts(Long id) {
        return executeWithinPersistenceContextReturningResult(entityManager -> entityManager
                .createQuery("SELECT c FROM Company c left join fetch c.products WHERE c.id=:id", Company.class)
                .setParameter("id", id)
                .getSingleResult());
    }

    private <T> T executeWithinPersistenceContextReturningResult(Function<EntityManager, T> emFuntion) {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        entityManager.unwrap(Session.class).setDefaultReadOnly(true);
        entityManager.getTransaction().begin();
        try {
            T result = emFuntion.apply(entityManager);
            entityManager.getTransaction().commit();
            return result;
        } catch (Exception e) {
            entityManager.getTransaction().rollback();
            throw new CompanyDaoException("Exception occurred during transaction...", e);
        } finally {
            entityManager.close();
        }
    }
}
