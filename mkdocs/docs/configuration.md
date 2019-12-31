# Configure Connector

The configuration files are in **<InstallRoot>/volume/config** directory. The following configuration files
need to be created to configure the connector:

- connection.json: Contains connection related configuration items to connect
                   to wiotp, data lake and scada historian.
- &lt;entityType&gt;.json: Contains entity data (extracted from SCADA historian) transformation 
                   and mapping configuration.

For details on connection and entity type configuration items, refer to the following sections:

- [Connection Configuration](connection.md)
- [Entity Type Configuration](data.md)


