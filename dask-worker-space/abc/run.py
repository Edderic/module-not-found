import logging

import coiled

from dask.distributed import Client, LocalCluster, UploadDirectory, as_completed

from fgh.ijk import use_add_1_twice

# Structure
# abc              FOLDER
#   run.py
#   cde.py
#   fgh            FOLDER
#     ijk.py

try:
    if __name__ == '__main__':
        cluster = LocalCluster(n_workers=1)
        # cluster = coiled.Cluster(
            # n_workers=3,
            # software="pan_ds_dask_env"
        # )

        client = Client(cluster)
        client.register_worker_plugin(
            UploadDirectory(
                "abc",
                update_path=True,
                restart=True
            ),
            nanny=True
        )

        future = client.submit(use_add_1_twice, 1)
        futures = [future]

        for f in as_completed(futures):
            print(f'result: {f.result()}')
except Exception as e:
    logging.exception(e)
    client.close()
    cluster.close()
