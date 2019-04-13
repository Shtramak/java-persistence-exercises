package ua.procamp.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.MapsId;
import javax.persistence.OneToOne;
import javax.persistence.Table;

/**
 * todo:
 * - implement not argument constructor
 * - implement getters and setters
 * - implement equals and hashCode based on identifier field
 * <p>
 * - configure JPA entity
 * - specify table name: "employee_profile"
 * - configure not nullable columns: position, department
 * <p>
 * - map relation between {@link Employee} and {@link EmployeeProfile} using foreign_key column: "employee_id"
 * - configure a derived identifier. E.g. map "employee_id" column should be also a primary key (id) for this entity
 */
@NoArgsConstructor
@Getter
@Setter
@EqualsAndHashCode(of = "id")
@Entity
@Table(name = "employee_profile")
public class EmployeeProfile {
    @Id
    private Long id;
    @OneToOne(optional = false)
    @JoinColumn(name = "employee_id")
    @MapsId
    private Employee employee;
    @Column(nullable = false)
    private String position;
    @Column(nullable = false)
    private String department;
}
