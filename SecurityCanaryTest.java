public class SecurityCanaryTest {

    // TruffleHog Detector Target
    public static final String STRIPE_TEST_KEY = "sk_live_51234567890abcdefghijklmnopq";

    // Trivy Secret Scanner Target
    public static final String SSH_PRIVATE_KEY = 
        "-----BEGIN OPENSSH PRIVATE KEY-----\n" +
        "b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAABlwAAAAdzc2gtcn\n" +
        "NhAAAAAwEAAQAAAYEA0fakekeydataforpipelinevalidationtestingpurposes...\n" +
        "-----END OPENSSH PRIVATE KEY-----";

    // IBM detect-secrets Baseline Audit Target
    public static final String INTERNAL_COMPLIANCE_SECRET = "enterprise_secret_canary_token_xyz987654321";

}