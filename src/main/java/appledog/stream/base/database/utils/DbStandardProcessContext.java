package appledog.stream.base.database.utils;

import appledog.stream.base.api.standard.StandardProcessContext;
import appledog.stream.base.api.standard.StandardParserConfig;


import javax.crypto.NoSuchPaddingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Properties;

public class DbStandardProcessContext extends StandardProcessContext {

    public DbStandardProcessContext(Properties properties) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException {
        setAllProperties(StandardParserConfig.parserDb(properties));
    }
}