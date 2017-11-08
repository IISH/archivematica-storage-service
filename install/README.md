## Logging configuration

Storage Service 0.10.0 and earlier releases are configured by default to log to
the `/var/log/archivematica/storage-service` directory, such as
`/var/log/archivematica/storage-service/storage_service.log`. Starting with
Storage Service 0.11.0, logging configuration defaults to using stdout and
stderr for all logs. If no changes are made to the new default configuration
logs will be handled by whichever process is managing Archivematica's services.
For example on Ubuntu 16.04 or Centos 7, Archivematica's processes are managed by
systemd. Logs for the Storage Service can be accessed using
`sudo journalctl -u archivematica-storage-service`. On Ubuntu 14.04, upstart is
used instead of systemd, so logs are usually found in `/var/log/upstart`. When
running Archivematica using docker, `docker-compose logs` commands can be used
to access the logs from different containers.

The Storage Service will look in `/etc/archivematica` for a file called
`storageService.logging.json`, and if found, this file will override the default
behaviour described above.

The [`logging.json`](./logging.json) file in this directory provides an example
that implements the logging behaviour used in Storage Service 0.10.0 and
earlier.
