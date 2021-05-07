package cn.springboot.controller;

import org.springframework.stereotype.Controller;
import org.springframework.util.ObjectUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpSession;
import java.util.Map;

/**
 * @author 魏喜明 2021-05-02 11:57:35
 */
@Controller
public class LoginController {
    @RequestMapping({"/", "/login", "/login.html"})
    public String index() {
        return "login";
    }

    @PostMapping("/user/login")
    public String login(
            @RequestParam("username") String username,
            @RequestParam("password") String password,
            Map<String, Object> map,
            HttpSession session
    ) {
        if (!ObjectUtils.isEmpty(username) && "000".equals(password)) {
            session.setAttribute("loginUser", username);
            return "redirect:/dashboard.html";
        } else {
            map.put("msg", "用户名密码错误！");
            return "login";
        }
    }
}
