/*
 * (C) Copyright 2011-2012 Nuxeo SA (http://nuxeo.com/) and contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *     Mathieu Guillaume
 *     Florent Guillaume
 */
package org.nuxeo.ecm.core.storage.sql;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.runtime.api.Framework;


/**
 * A Binary Manager that stores binaries as S3 BLOBs
 * <p>
 * The BLOBs are cached locally on first access for efficiency.
 * <p>
 * Because the BLOB length can be accessed independently of the binary stream,
 * it is also cached in a simple text file if accessed before the stream.
 */
public class S3BinaryManager extends AbstractS3BinaryManager  {

    private static final Log log = LogFactory.getLog(S3BinaryManager.class);

    public static final String BUCKET_NAME_KEY = "nuxeo.s3storage.bucket";

    public static final String BUCKET_REGION_KEY = "nuxeo.s3storage.region";

    public static final String DEFAULT_BUCKET_REGION = null; // US East

    public static final String AWS_ID_KEY = "nuxeo.s3storage.awsid";

    public static final String AWS_ID_ENV_KEY = "AWS_ACCESS_KEY_ID";

    public static final String AWS_SECRET_KEY = "nuxeo.s3storage.awssecret";

    public static final String AWS_SECRET_ENV_KEY = "AWS_SECRET_ACCESS_KEY";

    public static final String CACHE_SIZE_KEY = "nuxeo.s3storage.cachesize";

    public static final String DEFAULT_CACHE_SIZE = "100 MB";

    public static final String KEYSTORE_FILE_KEY = "nuxeo.s3storage.crypt.keystore.file";

    public static final String KEYSTORE_PASS_KEY = "nuxeo.s3storage.crypt.keystore.password";

    public static final String PRIVKEY_ALIAS_KEY = "nuxeo.s3storage.crypt.key.alias";

    public static final String PRIVKEY_PASS_KEY = "nuxeo.s3storage.crypt.key.password";

    // TODO define these constants globally somewhere
    public static final String PROXY_HOST_KEY = "nuxeo.http.proxy.host";


    @Override
    public Binary getBinary(InputStream in) throws IOException {

        TempEntry tmpEntry = doTempStorage(in);
        String digest = tmpEntry.digest;

        File cachedFile = fileCache.getFile(digest);
        if (cachedFile != null) {
            if (Framework.isTestModeSet()) {
                Framework.getProperties().setProperty("cachedBinary", digest);
            }
            return new Binary(cachedFile, digest, repositoryName);
        }

        doStoreInS3(tmpEntry);

        // Register the file in the file cache if all went well
        File file = fileCache.putFile(digest, tmpEntry.tmpFile);

        return new Binary(file, digest, repositoryName);
    }


}
