package org.nuxeo.ecm.core.storage.sql;

import java.io.File;
import java.io.IOException;

import org.nuxeo.ecm.core.work.AbstractWork;
import org.nuxeo.ecm.core.work.api.WorkManager;
import org.nuxeo.runtime.api.Framework;

/**
 * Variant of the S3 Binary Manager that flushes the stream to S3
 * using an asynchronous worker.
 *
 *
 * @author <a href="mailto:tdelprat@nuxeo.com">Tiry</a>
 */
public class AsyncS3BinaryManager extends S3BinaryManager {

    @Override
    protected FileStorage getStorage() {
        return new S3AsyncFileStorage();
    }

    public class S3AsyncFileStorage extends S3FileStorage {

        protected void doStoreFile(String digest, File file) throws IOException {
            super.storeFile(digest, file);
        }

        @Override
        public void storeFile(final String digest, final File file) throws IOException {

            WorkManager wm = Framework.getLocalService(WorkManager.class);
            wm.schedule(new AbstractWork() {
                @Override
                public String getTitle() {
                    return "S3 async writer for blob " + digest;
                }

                @Override
                public void work() throws Exception {
                    doStoreFile(digest, file);
                }
            });
        }
    }
}
