package ua.procamp.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * todo:
 * - implement no argument constructor
 * - implement getters and setters
 * - implement equals and hashCode based on identifier field
 * <p>
 * - configure JPA entity
 * - specify table name: "employee"
 * - configure auto generated identifier
 * - configure not nullable columns: email, firstName, lastName
 * <p>
 * - map unidirectional relation between {@link Employee} and {@link EmployeeProfile} on the child side
 */
@NoArgsConstructor
@Getter
@Setter
@EqualsAndHashCode(of = "id")
@Entity
@Table(name = "employee")
public class Employee {
    @Id
    @GeneratedValue
    private Long id;
    @Column(nullable = false)
    private String email;
    @Column(name = "first_name", nullable = false)
    private String fistName;
    @Column(name = "last_name", nullable = false)
    private String lastName;
}
