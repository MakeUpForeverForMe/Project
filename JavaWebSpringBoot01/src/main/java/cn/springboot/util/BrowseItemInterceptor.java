package cn.springboot.util;

import cn.springboot.entities.IpVo;
import cn.springboot.entities.SinaIpVo;
import com.google.gson.Gson;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Enumeration;

/**
 * @author 魏喜明 2021-05-06 07:16:06
 */
@Component
public class BrowseItemInterceptor implements HandlerInterceptor {

//    @Autowired
    RestTemplate restTemplate;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        Enumeration<String> headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            System.out.println(headerNames.nextElement());
        }
        System.out.println(request.getHeader("host"));
        String ip = request.getHeader("x-forwarded-for");
        System.out.println(ip);
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        if (ip != null && ip.length() != 0 && !"unknown".equalsIgnoreCase(ip)) {
            // 多次反向代理后会有多个ip值，第一个ip才是真实ip
            if (ip.contains(",")) {
                ip = ip.split(",")[0];
            }
        }
        //新浪查询失败查询阿里
        String sina = restTemplate.getForObject("http://int.dpool.sina.com.cn/iplookup/iplookup.php?format=json&ip={ip}", String.class, ip);
        SinaIpVo sinaIpVo = new Gson().fromJson(sina, SinaIpVo.class);
        if (sinaIpVo.getRet() != -1) {
            System.out.println(sinaIpVo.getProvince());
            System.out.println(sinaIpVo.getCity());
        } else {
            String object = restTemplate.getForObject("http://ip.taobao.com/service/getIpInfo.php?ip={ip}", String.class, ip);
            IpVo ipVo = new Gson().fromJson(object, IpVo.class);
            // XX表示内网
            if (ipVo.getCode() == 0 && !ipVo.getAddress().getRegion().equals("XX")) {
                System.out.println(ipVo.getAddress().getRegion());
                System.out.println(ipVo.getAddress().getCity());
            }
        }
        return true;
    }
}