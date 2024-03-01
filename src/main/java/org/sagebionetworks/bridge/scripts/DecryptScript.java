package org.sagebionetworks.bridge.scripts;

import static org.sagebionetworks.bridge.helper.LogHelper.logInfo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import com.google.common.io.ByteStreams;
import org.apache.commons.io.FileUtils;

import org.sagebionetworks.bridge.crypto.BcCmsEncryptor;
import org.sagebionetworks.bridge.crypto.PemUtils;

public class DecryptScript {
    private static final String BASE_DIRECTORY = "/Users/dwaynejeng/Downloads/";

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            logInfo("Usage: DecryptScript [path to public key] [path to private key] [path to file to decrypt] " +
                    "[path to decrypted file]");
            return;
        }

        // Set up encryptor.
        String certPem = FileUtils.readFileToString(new File(BASE_DIRECTORY + args[0]),
                "UTF-8");
        X509Certificate cert = PemUtils.loadCertificateFromPem(certPem);

        String privKeyPem = FileUtils.readFileToString(new File(BASE_DIRECTORY + args[1]),
                "UTF-8");
        PrivateKey privKey = PemUtils.loadPrivateKeyFromPem(privKeyPem);

        BcCmsEncryptor encryptor = new BcCmsEncryptor(cert, privKey);

        // Decrypt file.
        try (FileInputStream encryptedStream = new FileInputStream(BASE_DIRECTORY + args[2]);
                InputStream decryptedStream = encryptor.decrypt(encryptedStream);
                FileOutputStream decryptedOutStream = new FileOutputStream(BASE_DIRECTORY + args[3])) {
            ByteStreams.copy(decryptedStream, decryptedOutStream);
        }
    }
}
