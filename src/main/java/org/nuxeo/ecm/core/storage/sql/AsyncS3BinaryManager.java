package org.nuxeo.ecm.core.storage.sql;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.nuxeo.ecm.core.work.AbstractWork;
import org.nuxeo.ecm.core.work.api.WorkManager;
import org.nuxeo.runtime.api.Framework;

public class AsyncS3BinaryManager extends AbstractS3BinaryManager {

    @Override
    public Binary getBinary(InputStream in) throws IOException {

        final TempEntry tmpEntry = doTempStorage(in);
        final String digest = tmpEntry.digest;

        File cachedFile = fileCache.getFile(digest);
        if (cachedFile != null) {
            if (Framework.isTestModeSet()) {
                Framework.getProperties().setProperty("cachedBinary", digest);
            }
            return new Binary(cachedFile, digest, repositoryName);
        }

        WorkManager wm = Framework.getLocalService(WorkManager.class);
        wm.schedule(new AbstractWork() {
            @Override
            public String getTitle() {
                return "S3 async writer for blob " + digest;
            }

            @Override
            public void work() throws Exception {
                doStoreInS3(tmpEntry);
            }
        });

        // Register the file in the file cache if all went well
        File file = fileCache.putFile(digest, tmpEntry.tmpFile);

        return new Binary(file, digest, repositoryName);
    }

}
