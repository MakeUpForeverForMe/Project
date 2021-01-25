package netty2;

import cn.netty2.SimpleClient;
import cn.netty2.SimpleServer;
import org.junit.Test;

/**
 * @author 魏喜明
 */
public class SimpleTest {
    @Test
    public void simpleTestServer() {
        new SimpleServer(9999).run();
    }

    @Test
    public void simpleTestClient() throws Exception {
        new SimpleClient().connect("127.0.0.1", 9999);
    }
}
