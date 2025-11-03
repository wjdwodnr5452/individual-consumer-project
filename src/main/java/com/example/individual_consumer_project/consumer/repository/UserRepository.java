package com.example.individual_consumer_project.consumer.repository;

import com.example.individual_consumer_project.consumer.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;


public interface UserRepository extends JpaRepository<User, Long> {

    @Query("select u.name from User u where u.id = :id")
    String findNameById(@Param("id") Long id);

}
