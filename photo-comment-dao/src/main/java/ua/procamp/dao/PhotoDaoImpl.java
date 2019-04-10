package ua.procamp.dao;

import org.hibernate.Session;
import ua.procamp.model.Photo;
import ua.procamp.model.PhotoComment;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Please note that you should not use auto-commit mode for your implementation.
 */
public class PhotoDaoImpl implements PhotoDao {
    private EntityManagerFactory entityManagerFactory;

    public PhotoDaoImpl(EntityManagerFactory entityManagerFactory) {
        this.entityManagerFactory = entityManagerFactory;
    }

    @Override
    public void save(Photo photo) {
        executeWithinPersistenceContext(entityManager -> entityManager.persist(photo));
    }

    @Override
    public Photo findById(long id) {
        return executeWithtinPersistenceContextReturningResult(entityManager -> entityManager.find(Photo.class, id));
    }

    @Override
    public List<Photo> findAll() {
        return executeWithtinPersistenceContextReturningResult(entityManager -> entityManager
                .createQuery("SELECT p FROM Photo p", Photo.class).getResultList());
    }

    @Override
    public void remove(Photo photo) {
        executeWithinPersistenceContext(entityManager -> {
            Photo managedPhoto = entityManager.merge(photo);
            entityManager.remove(managedPhoto);
        });
    }

    @Override
    public void addComment(long photoId, String comment) {
        executeWithinPersistenceContext(entityManager -> {
            Photo photoReference = entityManager.getReference(Photo.class, photoId);
            PhotoComment photoComment = new PhotoComment();
            photoComment.setText(comment);
            photoReference.addComment(photoComment);
        });
    }

    private <T> T executeWithtinPersistenceContextReturningResult(Function<EntityManager, T> emFunction) {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        entityManager.unwrap(Session.class).setDefaultReadOnly(true);
        entityManager.getTransaction().begin();
        try {
            T result = emFunction.apply(entityManager);
            entityManager.getTransaction().commit();
            return result;
        } finally {
            entityManager.close();
        }
    }

    private void executeWithinPersistenceContext(Consumer<EntityManager> emConsumer) {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        entityManager.getTransaction().begin();
        try {
            emConsumer.accept(entityManager);
            entityManager.getTransaction().commit();
        } catch (Exception e) {
            entityManager.getTransaction().rollback();
            throw new RuntimeException(e);
        } finally {
            entityManager.close();
        }
    }
}