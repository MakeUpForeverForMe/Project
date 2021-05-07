package cn.springboot.component;

import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @author 魏喜明 2021-05-02 16:33:10
 * 登录检查
 */
public class LoginHandlerInterceptor implements HandlerInterceptor {
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        Object loginUser = request.getSession().getAttribute("loginUser");
        if (loginUser == null) {
            request.setAttribute("msg", "未登录，请先登录！");
            request.getRequestDispatcher("/login.html").forward(request, response);
            return false;
        } else {
            return true;
        }
    }
}
