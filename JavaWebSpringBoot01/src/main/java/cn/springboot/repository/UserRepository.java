package cn.springboot.repository;

import cn.springboot.entities.User;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author 魏喜明 2021-05-05 16:35:54
 */
public interface UserRepository extends JpaRepository<User, Integer> {
}
