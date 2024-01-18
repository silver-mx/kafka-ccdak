package dns.demo.kafka.java;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@ExtendWith(OutputCaptureExtension.class)
public class LoggingTest {

    // Contains data sent so System.out during the test.
    private ByteArrayOutputStream systemOutContent;
    private final PrintStream originalSystemOut = System.out;

    /**
     * This is only required for testLoggingPlainJava()
     */
    //@BeforeEach
    public void setUpStreams() {
        systemOutContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(systemOutContent));
        System.setErr(new PrintStream(systemOutContent));
    }

    /**
     * This is only required for testLoggingPlainJava()
     */
    //@AfterEach
    public void restoreStreams() {
        System.setOut(originalSystemOut);
    }

    @Test
    void testLoggingPlainJava() {
        String sl4jLog = "This is a sl4j log";
        String sysoutLog = "This is a sysout log";
        String syserrLog = "This is a syserr log";

        // This can be called as @BeforeEach but it was moved to the test to avoid affecting testLoggingOutputCaptureExtension()
        setUpStreams();

        log.info(sl4jLog);
        System.out.println(sysoutLog);
        System.err.println(syserrLog);

        assertThat(systemOutContent.toString()).contains(sl4jLog);
        assertThat(systemOutContent.toString()).contains(sysoutLog);
        assertThat(systemOutContent.toString()).contains(syserrLog);

        // This can be called as @BeforeEach but it was moved to the test to avoid affecting testLoggingOutputCaptureExtension()
        restoreStreams();
    }

    /**
     * https://docs.spring.io/spring-boot/docs/current/api/org/springframework/boot/test/system/OutputCaptureExtension.html
     * <p>
     * NOTE1: In log4j2.xml, remember to use "follow=true" in the Console appender, like:
     *
     * <Console name="CONSOLE" target="SYSTEM_OUT" follow="true">
     * <PatternLayout pattern="%date{yyyy-MM-dd HH:mm:ss.SSS}{UTC}Z %-5level [%15.15thread] %-40.40logger{1.} %msg%n"/>
     * </Console>
     * <p>
     * NOTE2: To reliably capture output from Java Util Logging, reset its configuration after each test:
     *
     * @AfterEach void reset() throws Exception {
     * LogManager.getLogManager().readConfiguration();
     * }
     */
    @Test
    void testLoggingOutputCaptureExtension(CapturedOutput output) {
        String sl4jLog = "This is a sl4j log";
        String sysoutLog = "This is a sysout log";
        String syserrLog = "This is a syserr log";

        log.info(sl4jLog);
        System.out.println(sysoutLog);
        System.err.println(syserrLog);

        assertThat(output.getOut()).contains(sl4jLog);
        assertThat(output.getOut()).contains(sysoutLog);
        assertThat(output.getErr()).contains(syserrLog);
    }
}
