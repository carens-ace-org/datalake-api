public class SecurityCanaryTest {


    // Trivy Secret Scanner Target
    public static final String SSH_PRIVATE_KEY = 
        "-----BEGIN OPENSSH PRIVATE KEY-----\n" +
        "b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAABlwAAAAdzc2gtcn\n" +
        "NhAAAAAwEAAQAAAYEA0fakekeydataforpipelinevalidationtestingpurposes...\n" +
        "-----END OPENSSH PRIVATE KEY-----";

    // IBM detect-secrets Baseline Audit Target
    public static final String INTERNAL_COMPLIANCE_SECRET = "enterprise_secret_canary_token_xyz987654321";

}