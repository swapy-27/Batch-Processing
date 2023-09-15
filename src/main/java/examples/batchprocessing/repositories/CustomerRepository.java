package examples.batchprocessing.repositories;

import examples.batchprocessing.entities.Customer;
import org.springframework.data.jpa.repository.JpaRepository;

import org.springframework.stereotype.Repository;


@Repository
public interface CustomerRepository extends JpaRepository<Customer,Integer> {
}
